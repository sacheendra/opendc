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
    val queues = ArrayList<ChannelQueue>()
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
        val queuesToAdd = toAdd.map {
            val cq = ChannelQueue(it)
            hostQueues[it.hostId] = cq
            cq
        }
        queues.addAll(queuesToAdd)
        nodeSelector.addNodes(queuesToAdd)

        queues.forEachIndexed { index, cq ->
            cq.idx = index
        }

        // Start listening on queue after creating all queues
        // Otherwise, items may be sent to queues which are not being read
        for (host in toAdd) {
            newHostsChannel.send(host)
        }
    }

    suspend fun removeHosts(toRemove: List<CacheHost>) {
        hosts.removeAll(toRemove)
        val queuesToRemove = toRemove.map { hostQueues[it.hostId]!! }
        queues.removeAll(queuesToRemove)
        nodeSelector.removeNodes(queuesToRemove)

        queuesToRemove.forEach {
            it.closed = true
            hostQueues.remove(it.host.hostId)
        }

        // Drain queues after they have been removed
        queuesToRemove.forEach { queue ->
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
                    task.hostId = chosenQueue.host.hostId
                    return task
                }
            }

            queue.wait()
            task = queue.next()
        }

        task?.hostId = queue.host.hostId
        return task
    }

    suspend fun offerTask(task: CacheTask) {
        // Decide host
        val queue = nodeSelector.getNode(task.objectId.toString()) as ChannelQueue

        queue.q.add(task)
        queue.pleaseNotify()
    }

    fun complete() {
        for (queue in hostQueues.values) {
            queue.closed = true
        }

        newHostsChannel.close()
    }
}

class ChannelQueue(h: CacheHost): Node {
//    val c: Channel<CacheTask> = Channel(Channel.UNLIMITED)
    val q: PriorityQueue<CacheTask> = PriorityQueue { a, b -> (a.submitTime - b.submitTime).toInt() }
    var idx: Int = -1
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

    override fun name(): String {
        return host.hostId.toString()
    }

    override fun compareTo(other: Node?): Int {
        other as ChannelQueue
        return host.hostId.compareTo(other.host.hostId)
    }
}
