package org.opendc.storage.remote

import io.ktor.server.routing.*
import io.ktor.server.response.*
import io.ktor.server.application.*
import io.ktor.server.request.receiveText
import kotlinx.coroutines.sync.Semaphore
import java.time.Clock
import java.time.LocalDate
import java.time.LocalDateTime

fun Application.configureRouting() {
    val addNodeLock = Semaphore(1)

    routing {

        post("/") {
            val msg = call.receiveText()
            val splits = msg.split(",")

            val action = splits[0]
            println("${LocalDateTime.now()} Event: $action")
            val res = when(action) {
                "ADD" -> {
                    addNodeLock.acquire()
                    val res = GlobalScheduler.addHost(splits[1])
                    addNodeLock.release()
                    res
                }
                "OFFER" -> {
                    GlobalScheduler.offerTask(splits[1], splits[2], splits[3].toLong(), splits[4].toLong(), splits[5])
                    "DONE"
                }
                "NEXT" -> {
                    GlobalScheduler.nextTask(splits[1])
                }
                else -> {
                    println("${LocalDateTime.now()} Unknown API call: $action")
                    "SAD"
                }
            }

            call.respondText(res)
        }
    }
}
