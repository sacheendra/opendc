package org.opendc.storage.cache.schedulers

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.opendc.storage.cache.CacheHost
import org.opendc.storage.cache.CacheTask
import org.opendc.storage.cache.ChannelQueue
import org.opendc.storage.cache.TaskScheduler
import java.util.PriorityQueue
import kotlin.time.Duration

class CentralizedDataAwarePlacer(
    val period: Duration
): ObjectPlacer {

    val keyToNodeMap = mutableMapOf<Long, CacheHost>()
    val nodeToKeyMap = mutableMapOf<CacheHost, Long>()
    val globalQueue = ChannelQueue(null)
//    val hostQueueHeap: PriorityQueue<ChannelQueue> = PriorityQueue { a, b -> a.q.size - b.q.size }

    var complete = false
    val thisFlow = flow<Unit> {
        delay(period)
        while (!complete) {
            rebalance()
            emit(Unit)
            delay(period)
        }
    }

    lateinit var scheduler: TaskScheduler
    fun getNode(key: Long): CacheHost? {
        return keyToNodeMap.getOrElse(key) {
            val emptyNodes = scheduler.hostQueues.filter { it.value.q.size == 0 }
            return if (emptyNodes.size > 0) {
                emptyNodes.toList().random().second.host
            } else {
                // Adds the task to the global queue which will then be pulled
                null
            }
        }
    }

    override fun addHosts(hosts: List<CacheHost>) {
        TODO("Not yet implemented")
    }

    override fun removeHosts(hosts: List<CacheHost>) {
        TODO("Not yet implemented")
    }

    override fun registerScheduler(scheduler: TaskScheduler) {
        this.scheduler = scheduler
    }

    override fun getPlacerFlow(): Flow<Unit>? {
        return thisFlow
    }

    override fun complete() {
        complete = true
    }

    override suspend fun getNextTask(host: CacheHost): CacheTask? {
        val queue = scheduler.hostQueues[host.hostId]
        if (queue == null) return null // This means the node has been deleted

        if (queue.closed) return null

        val minQueue = listOf(queue, globalQueue).minByOrNull {
            if (it.q.size > 0) {
                it.q.peek().submitTime
            } else {
                Long.MAX_VALUE
            }
        }

        val task = if (minQueue == null) {
            queue.wait()
            queue.next()
        } else {
            minQueue.next()
        }

        if (task != null) {
            task.hostId = host.hostId
            keyToNodeMap[task.objectId] = host
        }

        return task
    }

    override fun offerTask(task: CacheTask) {
        val host = getNode(task.objectId)
        if (host == null) {
            globalQueue.q.add(task)
            return
        }

        val chosenHostId = host.hostId
        val queue = scheduler.hostQueues[chosenHostId]!!

        queue.q.add(task)
        queue.pleaseNotify()
    }

    fun rebalance() {

    }

}
