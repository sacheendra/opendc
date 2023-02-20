package org.opendc.storage.cache.schedulers

import kotlinx.coroutines.flow.Flow
import org.opendc.storage.cache.CacheHost
import org.opendc.storage.cache.TaskScheduler

class GreedyObjectPlacer: ObjectPlacer {

    lateinit var scheduler: TaskScheduler
    override fun getNode(key: Long): CacheHost {
        val shortestQueue = scheduler.hostQueues.values
            .minBy { it.q.size }
        return shortestQueue.host
    }

    override fun addHosts(hosts: List<CacheHost>) {} // Not necessary

    override fun removeHosts(hosts: List<CacheHost>) {} // Not necessary

    override fun registerScheduler(scheduler: TaskScheduler) {
        this.scheduler = scheduler
    }

    override fun getPlacerFlow(): Flow<Unit>? {
        return null
    }

    override fun complete() {}

}

class RandomObjectPlacer: ObjectPlacer {

    lateinit var scheduler: TaskScheduler
    override fun getNode(key: Long): CacheHost {
        return scheduler.hosts.random()
    }

    override fun addHosts(hosts: List<CacheHost>) {} // Not necessary

    override fun removeHosts(hosts: List<CacheHost>) {} // Not necessary

    override fun registerScheduler(scheduler: TaskScheduler) {
        this.scheduler = scheduler
    }

    override fun getPlacerFlow(): Flow<Unit>? {
        return null
    }

    override fun complete() {}

}
