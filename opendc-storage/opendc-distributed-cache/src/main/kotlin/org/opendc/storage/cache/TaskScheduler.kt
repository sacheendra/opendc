package org.opendc.storage.cache

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import org.opendc.storage.cache.schedulers.ObjectPlacer
import java.util.PriorityQueue

class TaskScheduler(
    val stealWork: Boolean = true,
    val nodeSelector: ObjectPlacer
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
        nodeSelector.registerScheduler(this)
    }

    suspend fun addHosts(toAdd: List<CacheHost>) {
        hosts.addAll(toAdd)
        nodeSelector.addHosts(toAdd)
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
        nodeSelector.removeHosts(toRemove)
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
        return nodeSelector.getNextTask(host)
    }

    fun offerTask(task: CacheTask) {
        nodeSelector.offerTask(task)
    }

    fun complete() {
        for (queue in hostQueues.values) {
            queue.closed = true
        }

        nodeSelector.complete()
        newHostsChannel.close()
    }
}

class ChannelQueue(h: CacheHost?) {
//    val c: Channel<CacheTask> = Channel(Channel.UNLIMITED)
    val q: PriorityQueue<CacheTask> = PriorityQueue { a, b -> (a.submitTime - b.submitTime).toInt() }
    private val notifier: Mutex = Mutex()
    var closed: Boolean = false
        set(value) {
            field = value
            if (notifier.isLocked) notifier.unlock()
        }
    val host: CacheHost? = h

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
