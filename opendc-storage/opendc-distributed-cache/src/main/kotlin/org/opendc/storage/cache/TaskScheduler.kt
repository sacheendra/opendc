package org.opendc.storage.cache

import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ChannelResult
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.SelectClause1
import kotlinx.coroutines.sync.Mutex
import org.opendc.storage.cache.schedulers.ObjectPlacer
import java.util.PriorityQueue

class TaskScheduler(
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

    suspend fun removeHosts(toRemove: List<CacheHost>) {
        hosts.removeAll(toRemove)
        nodeSelector.removeHosts(toRemove)
        val queuesToDrain = toRemove.map { host ->
            // DONE: NEED TO RESCHEDULE TASKS AFTER REMOVING HOSTS
            // collect and re-offer tasks
//            println("closed ${host.hostId}")
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

    suspend fun offerTask(task: CacheTask) {
        nodeSelector.offerTask(task)
    }

    suspend fun complete() {
        for (queue in hostQueues.values) {
            queue.close()
        }

        nodeSelector.complete()
        newHostsChannel.close()
    }
}

class ChannelQueue(h: CacheHost?) {
    val c: Channel<Unit> = Channel(Channel.RENDEZVOUS)
    val q: PriorityQueue<CacheTask> = PriorityQueue { a, b -> (a.submitTime - b.submitTime).toInt() }

    private var closed: Boolean = false
    val host: CacheHost? = h

    suspend fun close() {
        closed = true
        pleaseNotify()
        c.close()
    }

    fun next(): CacheTask? {
        return if (q.size > 0) {
            q.remove()
        } else {
            null
        }
    }

    suspend fun add(task: CacheTask) {
        q.add(task)
        pleaseNotify()
    }

    suspend fun wait() {
        if (closed) return

        c.receiveCatching()
    }

    val selectWait: SelectClause1<ChannelResult<Unit>>
        get() = c.onReceiveCatching

    @OptIn(DelicateCoroutinesApi::class)
    suspend fun pleaseNotify() {
        if (!c.isClosedForReceive) c.trySend(Unit)
    }
}
