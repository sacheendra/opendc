package org.opendc.storage.cache

import ch.supsi.dti.isin.cluster.Node
import ch.supsi.dti.isin.consistenthash.ConsistentHash
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch
import org.opendc.storage.cache.schedulers.DynamicObjectPlacer
import kotlin.system.exitProcess

class TaskScheduler(
    val stealWork: Boolean = true,
    val nodeSelector: ConsistentHash
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

    init {
        if (nodeSelector is DynamicObjectPlacer) {
            nodeSelector.registerScheduler(this)
        }
    }

    suspend fun addHosts(toAdd: List<CacheHost>) {
        hosts.addAll(toAdd)
        nodeSelector.addNodes(toAdd)
        for (host in toAdd) {
            hostQueues[host.hostId] = ChannelQueue(host)
        }
        // Start listening on queue after creating all queues
        // Otherwise, items may be sent to queues which are not being read
        for (host in toAdd) {
            newHostsChannel.send(host)
        }
    }

    fun removeHosts(toRemove: List<CacheHost>) {
        hosts.removeAll(toRemove)
        nodeSelector.removeNodes(toRemove)
        toRemove.forEach<CacheHost> { host ->
            // DONE: NEED TO RESCHEDULE TASKS AFTER REMOVING HOSTS
            // collect and re-offer tasks
//            println("closed ${host.hostId}")
            val queue = hostQueues[host.hostId]!!
            queue.drain = true
            queue.c.close()
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    suspend fun getNextTask(host: CacheHost): CacheTask? {
        val queue = hostQueues[host.hostId]!!
        // Drain queue on node shut down
        if (queue.drain) {
            for (task in queue.c) {
                this.offerTask(task)
            }
            queue.size = 0
            hostQueues.remove(host.hostId)
            return null
        }

        var result = queue.c.tryReceive()

        if (result.isFailure && !result.isClosed) {
            if (stealWork) {
                // Work steal
                val chosenQueue = hostQueues.values
                    .filter { !it.drain } // Filter our closed nodes
                    .maxWith{a, b -> a.size - b.size}
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
            return null
        }

        queue.size--
        val task = result.getOrThrow()
        return task
    }

    suspend fun offerTask(task: CacheTask) {
        // Decide host
        val host = nodeSelector.getNode(task.objectId.toString()) as CacheHost
        val chosenHostId = host.hostId
        val queue = hostQueues[chosenHostId]!!

        val oldHostId = task.hostId
        task.hostId = chosenHostId

        queue.size++
        val res = queue.c.trySend(task)
        if (res.isFailure) {
            println(res.isClosed)
            println(queue.size)
            println(hosts.size)
            for (host in hosts) {
                println(hostQueues[host.hostId]!!.size)
            }
            println("failed send sourcehost: $oldHostId targethost: $chosenHostId drain: ${queue.drain}")
            exitProcess(1)
        }

    }

    fun complete() {
        for (h in hostQueues) {
            h.value.c.close()
        }

        newHostsChannel.close()
    }
}

class ChannelQueue(h: CacheHost) {
    val c: Channel<CacheTask> = Channel(Channel.UNLIMITED)
    var size: Int = 0
    var drain: Boolean = false
    val host: CacheHost = h
}
