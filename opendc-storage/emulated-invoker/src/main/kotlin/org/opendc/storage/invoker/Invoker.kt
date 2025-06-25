package org.opendc.storage.invoker

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.types.int
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withContext
import org.apache.commons.collections4.map.LRUMap
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.opendc.storage.cache.CacheTask
import org.opendc.storage.cache.CacheTaskWriteSupport
import org.opendc.storage.cache.RemoteStorage
import org.opendc.trace.util.parquet.LocalParquetWriter
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Clock

fun main(args: Array<String>) = Invoker().main(args)

class Invoker : CliktCommand() {
    val invokerIdStr: String by argument(help="Invoker ID")
    val concurrencyStr: String by argument(help="Concurrency")
    val schedulerURL: String by argument(help="Scheduler URL")
    val outputFolder: String by argument()

    override fun run() {
        val invokerId = invokerIdStr.toInt()
        val concurrency = concurrencyStr.toInt()

        val outputFolderPath = Paths.get(outputFolder)
        Files.createDirectories(outputFolderPath)

        // Setup metrics recorder
        val resultWriter = LocalParquetWriter.builder(outputFolderPath.resolve("invoker${invokerId}.parquet"), CacheTaskWriteSupport())
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .build()

        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() = runBlocking {
                println("Gracefully shutting down")
                resultWriter.close()
                println("Gracefully shut!!!")
            }
        })

        val remoteStorage = RemoteStorage()
        val cache: MutableMap<Long, Boolean> = LRUMap<Long, Boolean>(1000, 1000)

        val freeProcessingSlots = Semaphore(concurrency)

        runBlocking {
            val client = HttpClient(CIO){
                install(HttpTimeout) {
                    requestTimeoutMillis = HttpTimeout.INFINITE_TIMEOUT_MS
                    socketTimeoutMillis = HttpTimeout.INFINITE_TIMEOUT_MS
                }
            }

            val res = client.post(schedulerURL) {
                setBody("ADD,${invokerId}")
            }
            val registerBody = res.bodyAsText()
            if (registerBody != "DONE") {
//                throw Exception("Unable to register invoker ${invokerId}: $registerBody")
            }

            try {
                while (true) {
                    freeProcessingSlots.acquire()

                    launch {
                        val dummyStart = Clock.systemUTC().millis()
                        val nextRes = client.post(schedulerURL) {
                            setBody("NEXT,${invokerId}")
                        }
                        val nextBody = nextRes.bodyAsText()
                        if (nextBody == "") freeProcessingSlots.release()

                        val actualStart = Clock.systemUTC().millis()

                        val splits = nextBody.split(",")
                        val taskId = splits[0].toLong()
                        val objectId = splits[1].toLong()
                        val duration = splits[2].toLong()
                        val submitTime = splits[3].toLong()
                        val callbackUrl = splits[4]

                        var storageDelay = 0L
                        val objInCache = cache[objectId]
                        var isHit = true
                        if (objInCache == null) {
                            isHit = false
                            storageDelay = remoteStorage.retrieve(duration)
                            cache[objectId] = true
                        }
                        delay(storageDelay + duration)

                        freeProcessingSlots.release()

                        launch(Dispatchers.IO) {
                            resultWriter.write(
                                CacheTask(
                                    taskId,
                                    callbackUrl.toLong(),
                                    duration,
                                    submitTime,
                                    isHit = isHit,
                                    startTime = actualStart,
                                    endTime = Clock.systemUTC().millis(),
                                    storageDelay = dummyStart
                                )
                            )
                        }
                    }
                }
            } catch(e: Exception) {
                resultWriter.close()
                throw e
            }
        }
    }

}
