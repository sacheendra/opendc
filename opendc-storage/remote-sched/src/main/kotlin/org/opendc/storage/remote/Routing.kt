package org.opendc.storage.remote

import io.ktor.server.routing.*
import io.ktor.server.response.*
import io.ktor.server.application.*
import io.ktor.server.request.receiveText

fun Application.configureRouting() {
    routing {
        get("/") {
            call.respondText("Hello World!")
        }

        post("/") {
            val msg = call.receiveText()
            val splits = msg.split(",")

            val action = splits[0]
            val res = when(action) {
                "ADD" -> {
                    GlobalScheduler.addHost(splits[1])
                }
                "OFFER" -> {
                    GlobalScheduler.offerTask(splits[1], splits[2], splits[3].toLong(), splits[4])
                    "DONE"
                }
                "NEXT" -> {
                    GlobalScheduler.nextTask(splits[1])
                }
                else -> {
                    "Unknown API Call"
                }
            }

            call.respondText(res)
        }
    }
}
