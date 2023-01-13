package org.opendc.storage.cache

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch

class TaskScheduler(
    val stealWork: Boolean = true
) {
    val hosts = ArrayList<CacheHost>()
    val hostQueues = HashMap<Int, ChannelQueue>()
    val newHostsChannel = Channel<CacheHost>()

    val completedTaskFlow = channelFlow<CacheTask> {
        while (true) {
            val newHostResult = newHostsChannel.receiveCatching()
            if (newHostResult.isClosed) {
                return@channelFlow
            }

            val newHost = newHostResult.getOrThrow()
            launch {
                newHost.processTasks(channel)
            }
        }
    }

    suspend fun addHost(host: CacheHost) {
        hosts.add(host)
        hostQueues[host.hostId] = ChannelQueue()
        newHostsChannel.send(host)
    }

    fun removeHosts(toRemove: List<CacheHost>) {
        hosts.removeAll(toRemove)
        toRemove.forEach {
            // NEED TO RESCHEDULE TASKS AFTER REMOVING HOSTS
            // collect and reoffer tasks
            hostQueues[it.hostId]!!.c.close()
        }
    }

    suspend fun getNextTask(host: CacheHost): CacheTask? {
        val queue = hostQueues[host.hostId]!!
        var result = queue.c.tryReceive()

        if (result.isFailure && !result.isClosed) {
            if (stealWork) {
                // Work steal
//                val queuesToCheck = (1..2).map { hostQueues.values.random() }
                val chosenQueue = hostQueues.values.maxWith{a, b -> a.size - b.size}
//                println(chosenQueue.size)
                if (chosenQueue.size > 5) {
                    val task = chosenQueue.c.receive()
                    chosenQueue.size--
                    task.stolen = true
                    return task
                }
            }

            result = queue.c.receiveCatching()
        }

        if (result.isClosed) {
            // Clean up after self after node is stopped
            // MEMORY LEAK! But, its ok
            // Do we even need this, we start thousands of nodes at best
//            hostQueues.remove(host.hostId)
            return null
        }

        queue.size--
        val task = result.getOrThrow()
        return task
    }

    suspend fun offerTask(task: CacheTask) {
        // Decide host
        val chosenHostIndex = task.objectId % hosts.size
        val chosenHost = hosts[chosenHostIndex.toInt()]
        val queue = hostQueues[chosenHost.hostId]!!
        task.hostId = chosenHost.hostId

        queue.size++
        queue.c.send(task)
    }

    fun complete() {
        synchronized(hostQueues) {
            for (h in hostQueues) {
                h.value.c.close()
            }
        }

        newHostsChannel.close()
    }
}

class ChannelQueue {
    val c: Channel<CacheTask> = Channel(1000)
    var size: Int = 0
}
