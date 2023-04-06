package org.opendc.storage.loadgen

import io.ktor.server.routing.*
import io.ktor.server.response.*
import io.ktor.server.application.*
import io.ktor.server.request.receiveText

fun Application.configureRouting() {
    routing {

        post("/") {
            val msg = call.receiveText()
            val splits = msg.split(",")

            val action = splits[0]
            val res = when(action) {
                "FINISH" -> {
                    "DONE"
                }
                else -> {
                    throw Exception("Unknown API Call")
                }
            }

            call.respondText(res)
        }
    }
}
