package org.opendc.storage.cache

import org.opendc.simulator.kotlin.runSimulation

fun main(args: Array<String>) {
    runSimulation {
        // Setup remote storage
        val rs = RemoteStorage()
        for (i in 1..10) {
            println((CacheHost(4, 100, timeSource, rs, TaskScheduler())).hostId)
        }

        // Setup hosts

        // Setup scheduler

        // Setup service

        // Replay trace

        // Record metrics
    }
}
