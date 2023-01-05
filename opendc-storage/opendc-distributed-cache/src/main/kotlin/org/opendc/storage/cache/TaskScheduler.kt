package org.opendc.storage.cache

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch

class TaskScheduler {
    val hosts = ArrayList<CacheHost>()
    val hostQueues = HashMap<Int, ArrayDeque<CacheTask>>()

    fun addHost(host: CacheHost) {
        hosts.add(host)
        hostQueues[host.hostId] = ArrayDeque()
    }

    fun getNextTask(host: CacheHost): CacheTask? {
        val queue = hostQueues[host.hostId]!!
        return queue.removeFirstOrNull()
    }

    suspend fun offerTask(task: CacheTask) = coroutineScope {
        // Decide host
        val chosenHostIndex = task.objectId % hosts.size
        val chosenHost = hosts[chosenHostIndex.toInt()]
        val queue = hostQueues[chosenHost.hostId]!!
        task.hostId = chosenHost.hostId
        if (chosenHost.freeProcessingSlots > 0) {
            launch {
                chosenHost.runTask(task)
            }
        } else {
            queue.addLast(task)
        }
    }
}
