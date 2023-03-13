package org.opendc.storage.cache.schedulers

import kotlinx.coroutines.flow.Flow
import org.opendc.storage.cache.CacheHost
import org.opendc.storage.cache.CacheTask
import org.opendc.storage.cache.ChannelQueue
import org.opendc.storage.cache.TaskScheduler

class GreedyObjectPlacer: ObjectPlacer {

    override lateinit var scheduler: TaskScheduler
    val globalQueue = ChannelQueue(null)

    override fun addHosts(hosts: List<CacheHost>) {} // Not necessary

    override fun removeHosts(hosts: List<CacheHost>) {} // Not necessary

    override fun getPlacerFlow(): Flow<TimeCountPair>? {
        return null
    }

    override suspend fun complete() {
        globalQueue.close()
    }

    override suspend fun getNextTask(host: CacheHost): CacheTask? {
        var task = globalQueue.next()
//println(host.hostId)
        if (task == null) {
            globalQueue.wait()
            task = globalQueue.next()
        }

        task?.hostId = host.hostId
//        println(task?.taskId)

        return task
    }

    override suspend fun offerTask(task: CacheTask) {
        globalQueue.add(task)
    }

}

class RandomObjectPlacer: ObjectPlacer {

    override lateinit var scheduler: TaskScheduler
    fun getNode(): CacheHost {
        return scheduler.hosts.random()
    }

    override fun addHosts(hosts: List<CacheHost>) {} // Not necessary

    override fun removeHosts(hosts: List<CacheHost>) {} // Not necessary

    override fun getPlacerFlow(): Flow<TimeCountPair>? {
        return null
    }

    override suspend fun complete() {}

    override suspend fun getNextTask(host: CacheHost): CacheTask? {
        val queue = scheduler.hostQueues[host.hostId]
        if (queue == null) return null // This means the node has been deleted

        var task = queue.next()

        if (task == null) {
            queue.wait()
            task = queue.next()
        }

        task?.hostId = host.hostId

        return task
    }

    override suspend fun offerTask(task: CacheTask) {
        // Decide host
        val host = getNode()
        val chosenHostId = host.hostId
        val queue = scheduler.hostQueues[chosenHostId]!!

        queue.add(task)
    }

}
