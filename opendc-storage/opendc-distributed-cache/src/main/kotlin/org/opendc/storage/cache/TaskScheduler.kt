package org.opendc.storage.cache

import ch.supsi.dti.isin.cluster.Node
import ch.supsi.dti.isin.consistenthash.ConsistentHash
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.yield
import org.opendc.storage.cache.schedulers.DynamicObjectPlacer
import java.util.PriorityQueue
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

    suspend fun removeHosts(toRemove: List<CacheHost>) {
        hosts.removeAll(toRemove)
        nodeSelector.removeNodes(toRemove)
        val queuesToDrain = toRemove.map { host ->
            // DONE: NEED TO RESCHEDULE TASKS AFTER REMOVING HOSTS
            // collect and re-offer tasks
//            println("closed ${host.hostId}")
            val queue = hostQueues[host.hostId]!!
            queue.closed = true
            hostQueues.remove(host.hostId)!!
        }

        // Drain queues after they have been removed
        queuesToDrain.forEach { queue ->
            // Drain queue on node shut down
            for (task in queue.q) {
                this.offerTask(task)
            }
        }
    }

    suspend fun getNextTask(host: CacheHost): CacheTask? {
        val queue = hostQueues[host.hostId]
        if (queue == null) return null // This means the node has been deleted

        if (queue.closed) return null

        var task = queue.next()

        if (task == null) {
            if (stealWork) {
                // Work steal
                val chosenQueue = hostQueues.values
                    .maxWith{a, b -> a.q.size - b.q.size}
                if (chosenQueue.q.size > 5) {
                    val task = chosenQueue.next()!!
                    task.stolen = true
                    return task
                }
            }

            queue.wait()
            task = queue.next()
        }

        return task
    }

    suspend fun offerTask(task: CacheTask) {
        // Decide host
        val host = nodeSelector.getNode(task.objectId.toString()) as CacheHost
        val chosenHostId = host.hostId
        val queue = hostQueues[chosenHostId]!!

        val oldHostId = task.hostId
        task.hostId = chosenHostId

        queue.q.add(task)
        queue.pleaseNotify()
//        if (res.isFailure) {
//            println(res.isClosed)
//            println(queue.size)
//            println(hosts.size)
//            for (h in hosts) {
//                println(hostQueues[h.hostId]!!.size)
//            }
//            println("failed send sourcehost: $oldHostId targethost: $chosenHostId")
//            exitProcess(1)
//        }

    }

    fun complete() {
        for (queue in hostQueues.values) {
            queue.closed = true
        }

        newHostsChannel.close()
    }
}

class ChannelQueue(h: CacheHost) {
//    val c: Channel<CacheTask> = Channel(Channel.UNLIMITED)
    val q: PriorityQueue<CacheTask> = PriorityQueue { a, b -> (a.submitTime - b.submitTime).toInt() }
    private val notifier: Mutex = Mutex()
    var closed: Boolean = false
        set(value) {
            field = value
            if (notifier.isLocked) notifier.unlock()
        }
    val host: CacheHost = h

    fun next(): CacheTask? {
        return if (q.size > 0) {
            q.remove()
        } else null
    }

    suspend fun wait() {
        notifier.lock()
        // After locking wait for sender to unlock it
        notifier.lock()
        notifier.unlock()
    }

    fun pleaseNotify() {
        if (notifier.isLocked) notifier.unlock()
    }
}
