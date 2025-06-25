package org.opendc.storage.loadgen

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.default
import com.github.ajalt.clikt.parameters.types.int
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.cancellable
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.drop
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.opendc.storage.cache.CacheTask
import org.opendc.storage.cache.CacheTaskReadSupport
import org.opendc.storage.cache.CacheTaskWriteSupport
import org.opendc.trace.util.parquet.LocalParquetReader
import org.opendc.trace.util.parquet.LocalParquetWriter
import sun.misc.Signal
import java.lang.Long.max
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Clock
import kotlin.random.Random
import kotlin.system.exitProcess
import kotlin.time.Duration.Companion.seconds

fun main(args: Array<String>) = LoadGen().main(args)

fun Application.module() {
    configureRouting()
}

class LoadGen : CliktCommand() {
    val inputFilename: String by argument(help="Input trace")
    val schedulerURL: String by argument(help="Scheduler URL")
    val sampleIndex: Int by argument().int().default(4)

    override fun run() {

        runBlocking {
            val client = HttpClient(OkHttp){
                install(HttpTimeout) {
                    requestTimeoutMillis = Long.MAX_VALUE
                }
            }

            val startedReceiver = embeddedServer(Netty, port = 19002, host = "0.0.0.0", module = Application::module)
                .start(wait = false) //.start(wait = true)

            // Load trace
            val traceReadPath = Paths.get(inputFilename)
            val traceReader = LocalParquetReader(traceReadPath, CacheTaskReadSupport(), false)

            Signal.handle(Signal("INT")) {
                startedReceiver.stop()
                exitProcess(0)
            }

            val startTime = System.nanoTime() / 1e6.toLong()
            fun currentTime(): Long {
                return (System.nanoTime() / 1e6.toLong()) - startTime
            }

            var i=0
            while (true) {
                i++
                val task = traceReader.read()
                if (task == null) break

                if (i%sampleIndex == 0) {
                    delay(task.submitTime - currentTime())
                    launch {
                        client.post(schedulerURL) {
                            setBody(
                                "OFFER,${task.taskId},${task.objectId},${task.duration},${
                                    Clock.systemUTC().millis()
                                },http://localhost:19002"
                            )
                        }
                    }
                }
            }
        }

    }
}
