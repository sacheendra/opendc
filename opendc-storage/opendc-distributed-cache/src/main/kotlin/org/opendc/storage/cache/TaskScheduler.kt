package org.opendc.storage.cache

import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch
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
            val queue = hostQueues[host.hostId]!!
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
//    val c: Channel<CacheTask> = Channel(Channel.UNLIMITED)
    val q: PriorityQueue<CacheTask> = PriorityQueue { a, b -> (a.submitTime - b.submitTime).toInt() }

    val notifier: Mutex = Mutex()

    val notifier2: Channel<Unit> = Channel()
//    val notifierIn: Channel<Unit> = Channel(Channel.UNLIMITED)

//    var isLocked: AtomicBoolean = AtomicBoolean(false)
    private var closed: Boolean = false
    val host: CacheHost? = h

    suspend fun close() {
        closed = true
        pleaseNotify()
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

        notifier.lock()
//        isLocked.set(true)
        notifier.lock()
        notifier.unlock()
//        notifier2.receive()
    }

//    @OptIn(InternalCoroutinesApi::class)
//    val onReceive: SelectClause1<Unit>
//        get() = object : SelectClause1<Unit> {
//        @Suppress("UNCHECKED_CAST")
//        override fun <R> registerSelectClause1(select: SelectInstance<R>, block: suspend (Unit) -> R) {
//            val receiveMode = 0
//            while (true) {
//                if (select.isSelected) return
//
//                block
//
//                wait()
//                if (isEmptyImpl) {
//                    if (enqueueReceiveSelect(select, block, receiveMode)) return
//                } else {
//                    val pollResult = pollSelectInternal(select)
//                    when {
//                        pollResult === ALREADY_SELECTED -> return
//                        pollResult === POLL_FAILED -> {} // retry
//                        pollResult === RETRY_ATOMIC -> {} // retry
//                        else -> block.tryStartBlockUnintercepted(select, receiveMode, pollResult)
//                    }
//                }
//            }
//        }
//    }
//        get() {
////            isLocked = true
//            return notifier2.onReceive
//        }

    suspend fun pleaseNotify() {
//        if (isLocked.compareAndSet(true, false)) {
        if (notifier.isLocked) {
            notifier.unlock()
//            isLocked = false
//            notifier2.send(Unit)
        }
//        if (isLocked) notifier2.send(Unit)
    }
}

suspend fun multiQueueWait(queue1: ChannelQueue, queue2: ChannelQueue): ChannelQueue {
    val m = Mutex()
    var resultQueue: ChannelQueue = queue1
    m.lock()

    coroutineScope {
        var h1: Job? = null; var h2: Job? = null
        h1 = launch {
            if (!m.isLocked) return@launch
            queue1.wait()
            h2?.cancel()
            m.unlock()
            resultQueue = queue1
        }
        h2 = launch {
            if (!m.isLocked) return@launch
            queue2.wait()
            h1.cancel()
            m.unlock()
            resultQueue = queue2
        }
    }

    m.lock()
    m.unlock()

    return resultQueue
}
