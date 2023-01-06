package org.opendc.storage.cache

import kotlinx.coroutines.channels.Channel

class TaskScheduler {
    val hosts = ArrayList<CacheHost>()
    val hostQueues = HashMap<Int, Channel<CacheTask>>()

    fun addHost(host: CacheHost) {
        hosts.add(host)
        hostQueues[host.hostId] = Channel(1000)
    }

    suspend fun getNextTask(host: CacheHost): CacheTask? {
        val queue = hostQueues[host.hostId]!!
        val result = queue.receiveCatching()
        return result.getOrNull()
    }

    suspend fun offerTask(task: CacheTask) {
        // Decide host
        val chosenHostIndex = task.objectId % hosts.size
        val chosenHost = hosts[chosenHostIndex.toInt()]
        val queue = hostQueues[chosenHost.hostId]!!
        task.hostId = chosenHost.hostId
        queue.send(task)
    }

    fun complete() {
        for (h in hostQueues) {
            h.value.close()
        }
    }
}
