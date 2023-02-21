package org.opendc.storage.cache.schedulers

import kotlinx.coroutines.flow.Flow
import org.opendc.storage.cache.Autoscaler
import org.opendc.storage.cache.CacheHost
import org.opendc.storage.cache.CacheTask
import org.opendc.storage.cache.ChannelQueue
import org.opendc.storage.cache.TaskScheduler

class GreedyObjectPlacer: ObjectPlacer {

    override lateinit var scheduler: TaskScheduler
    override var autoscaler: Autoscaler? = null
    val globalQueue = ChannelQueue(null)

    override fun addHosts(hosts: List<CacheHost>) {} // Not necessary

    override fun removeHosts(hosts: List<CacheHost>) {} // Not necessary

    override fun getPlacerFlow(): Flow<Unit>? {
        return null
    }

    override fun complete() {
        globalQueue.closed = true
    }

    override suspend fun getNextTask(host: CacheHost): CacheTask? {
        if (globalQueue.closed) return null

        var task = globalQueue.next()

        if (task == null) {
            globalQueue.wait()
            task = globalQueue.next()
        }

        task?.hostId = host.hostId

        return task
    }

    override fun offerTask(task: CacheTask) {
        globalQueue.q.add(task)
        globalQueue.pleaseNotify()
    }

}

class RandomObjectPlacer: ObjectPlacer {

    override lateinit var scheduler: TaskScheduler
    override var autoscaler: Autoscaler? = null
    fun getNode(): CacheHost {
        return scheduler.hosts.random()
    }

    override fun addHosts(hosts: List<CacheHost>) {} // Not necessary

    override fun removeHosts(hosts: List<CacheHost>) {} // Not necessary

    override fun getPlacerFlow(): Flow<Unit>? {
        return null
    }

    override fun complete() {}

    override suspend fun getNextTask(host: CacheHost): CacheTask? {
        val queue = scheduler.hostQueues[host.hostId]
        if (queue == null) return null // This means the node has been deleted

        if (queue.closed) return null

        var task = queue.next()

        if (task == null) {
            queue.wait()
            task = queue.next()
        }

        task?.hostId = host.hostId

        return task
    }

    override fun offerTask(task: CacheTask) {
        // Decide host
        val host = getNode()
        val chosenHostId = host.hostId
        val queue = scheduler.hostQueues[chosenHostId]!!

        queue.q.add(task)
        queue.pleaseNotify()
    }

}
