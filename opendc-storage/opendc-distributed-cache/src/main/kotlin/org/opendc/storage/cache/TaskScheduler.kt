package org.opendc.storage.cache

import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.SelectBuilder
import kotlinx.coroutines.selects.SelectClause1
import kotlinx.coroutines.selects.SelectInstance
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
    val notifier2: Channel<Unit> = Channel()
    var isLocked: Boolean = false
    var closed: Boolean = false
        set(value) {
            field = value
            pleaseNotify()
        }
    val host: CacheHost? = h

    fun next(): CacheTask? {
        return if (q.size > 0) {
            q.remove()
        } else null
    }

    fun add(task: CacheTask) {
        q.add(task)
        pleaseNotify()
    }

    suspend fun wait() {
        isLocked = true
        notifier2.receive()
    }

    val onReceive: SelectClause1<Unit>
        get() {
            isLocked = true
            return notifier2.onReceive
        }

    fun pleaseNotify() {
        if (isLocked) notifier2.trySend(Unit)
    }
}
