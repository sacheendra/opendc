package org.opendc.storage.cache.schedulers

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.opendc.storage.cache.CacheHost
import org.opendc.storage.cache.TaskScheduler
import kotlin.time.Duration

class CentralizedDataAwarePlacer(
    val period: Duration
): ObjectPlacer {

    val keyToNodeMap = mapOf<Long, CacheHost>()
    val nodeToKeyMap = mapOf<CacheHost, Long>()

    var complete = false
    val placerFlow = flow<Unit> {
        delay(period)
        while (!complete) {
            rebalance()
            emit(Unit)
            delay(period)
        }
    }

    lateinit var scheduler: TaskScheduler
    override fun getNode(key: Long): CacheHost {
        return keyToNodeMap.getOrElse(key) {
            scheduler.hosts[0]
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
        return null
    }

    override fun complete() {
        complete = true
    }

    fun rebalance() {

    }

}
