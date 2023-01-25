package org.opendc.storage.cache

import ch.supsi.dti.isin.cluster.Node
import ch.supsi.dti.isin.consistenthash.ConsistentHash
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch

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
        }

        var result = queue.c.tryReceive()

        if (result.isFailure && !result.isClosed) {
            if (stealWork) {
                // Work steal
                val chosenQueue = hostQueues.values.maxWith{a, b -> a.size - b.size}
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
        task.hostId = chosenHostId

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

class ChannelQueue(h: CacheHost) {
    val c: Channel<CacheTask> = Channel(1000)
    var size: Int = 0
    var drain: Boolean = false
    val host: CacheHost = h
}

interface DynamicObjectPlacer {
    fun registerScheduler(scheduler: TaskScheduler)
}

class GreedyObjectPlacer: ConsistentHash, DynamicObjectPlacer {

    lateinit var scheduler: TaskScheduler
    override fun getNode(key: String?): Node {
        val shortestQueue = scheduler.hostQueues
            .filter { !it.value.drain } // Filter our closed nodes
            .minBy { it.value.size }
        return shortestQueue.value.host
    }

    override fun addNodes(nodes: MutableCollection<out Node>?) {
        return
    }

    override fun removeNodes(nodes: MutableCollection<out Node>?) {
        return
    }

    override fun nodeCount(): Int {
        return 0
    }

    override fun engine(): Any? {
        return null
    }

    override fun registerScheduler(scheduler: TaskScheduler) {
        this.scheduler = scheduler
    }

}
