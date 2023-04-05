package org.opendc.storage.remote

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.types.int
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import org.apache.commons.collections4.map.LRUMap
import org.opendc.storage.cache.RemoteStorage

fun main(args: Array<String>) = RemoteSched().main(args)

class RemoteSched : CliktCommand() {
    val invokerId: Int by argument(help="Invoker ID").int()
    val concurrency: Int by argument(help="Concurrency").int()
    val schedulerURL: String by argument(help="Scheduler URL")

    val remoteStorage = RemoteStorage()
    val cache: MutableMap<Long, Boolean> = LRUMap<Long, Boolean>(1000, 1000)

    val freeProcessingSlots = Semaphore(concurrency)

    override fun run() {
        runBlocking {
            val client = HttpClient(CIO)

            val res = client.post(schedulerURL) {
                setBody("ADD,${invokerId}")
            }
            val registerBody = res.bodyAsText()
            if (registerBody != "DONE") {
                throw Exception("Unable to register invoker ${invokerId}: $registerBody")
            }

            while (true) {
                freeProcessingSlots.acquire()

                val nextRes = client.post(schedulerURL) {
                    setBody("NEXT,${invokerId}")
                }
                val nextBody = nextRes.bodyAsText()

                val splits = nextBody.split(",")
                val taskId = splits[0].toLong()
                val objectId = splits[1].toLong()
                val duration = splits[2].toLong()
                val callbackUrl = splits[3]

                var storageDelay = 0L
                val objInCache = cache[objectId]
                if (objInCache == null) {
                    storageDelay = remoteStorage.retrieve(duration)
                    cache[objectId] = true
                }
                delay(storageDelay + duration)

                client.get(callbackUrl)

                freeProcessingSlots.release()
            }
        }
    }

}
