package org.opendc.storage.cache

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch

class TaskScheduler {
    val hosts = ArrayList<CacheHost>()
    val hostQueues = HashMap<Int, Channel<CacheTask>>()
    val newHostsChannel = Channel<CacheHost>()

    val completedTaskFlow = channelFlow<CacheTask> {
        while (true) {
            val newHostResult = newHostsChannel.receiveCatching()
            if (newHostResult.isClosed) {
                return@channelFlow
            }

            val newHost = newHostResult.getOrThrow()
            launch {
                while (true) {
                    val task = newHost.nextTask()
                    if (task != null) {
                        send(task)
                    } else {
                        break
                    }
                }
            }
        }
    }

    suspend fun addHost(host: CacheHost) {
        hosts.add(host)
        hostQueues[host.hostId] = Channel(1000)
        newHostsChannel.send(host)
    }

    fun removeHosts(toRemove: List<CacheHost>) {
        hosts.removeAll(toRemove)
        toRemove.forEach {
            hostQueues[it.hostId]!!.close()
        }
    }

    suspend fun getNextTask(host: CacheHost): CacheTask? {
        val queue = hostQueues[host.hostId]!!
        val result = queue.receiveCatching()
        val task = result.getOrNull()
        if (task == null) {
            // Clean up after self after node is stopped
            hostQueues.remove(host.hostId)
        }
        return task
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
        newHostsChannel.close()
    }
}
