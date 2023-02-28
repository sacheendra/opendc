package org.opendc.storage.cache.schedulers

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.opendc.storage.cache.Autoscaler
import org.opendc.storage.cache.CacheHost
import org.opendc.storage.cache.CacheTask
import org.opendc.storage.cache.ChannelQueue
import org.opendc.storage.cache.TaskScheduler
import kotlin.time.Duration

class DelegatedDataAwarePlacer(
    val period: Duration,
    val numSchedulers: Int,
    val minMovement: Boolean = false,
    val lookBackward: Boolean = false
): ObjectPlacer {

    override lateinit var scheduler: TaskScheduler
    override var autoscaler: Autoscaler? = null

    val placerToHostMap: List<MutableList<CacheHost>> = List(numSchedulers) { _ -> mutableListOf() }
    val hostToPlacerMap: MutableMap<CacheHost, Int> = mutableMapOf()
    val subPlacers: List<CentralizedDataAwarePlacer> = List(numSchedulers) { _ -> CentralizedDataAwarePlacer(period, minMovement) }
    val unallocatedHosts: MutableList<CacheHost> = mutableListOf()

    var complete = false
    val thisFlow = flow<Unit> {
        delay(period)
        while (!complete) {
            rebalance()
            emit(Unit)
            delay(period)
        }
    }

    override fun addHosts(hosts: List<CacheHost>) {
        unallocatedHosts.addAll(hosts)
        rebalance()
    }

    override fun removeHosts(hosts: List<CacheHost>) {
        for (host in hosts) {
            val placerIdx = hostToPlacerMap.remove(host)
            if (placerIdx != null) {
                val placerHosts = placerToHostMap[placerIdx]
                placerHosts.remove(host)
            } else if (unallocatedHosts.contains(host)) {
                unallocatedHosts.remove(host)
            }
            throw IllegalAccessError("Placer not found for host and host is not new")
        }
        rebalance()
    }

    override fun getPlacerFlow(): Flow<Unit>? {
        return thisFlow
    }

    override fun complete() {
        complete = true
    }

    override suspend fun getNextTask(host: CacheHost): CacheTask? {
        val placerIdx = hostToPlacerMap[host]!! // A host should always be linked to a placer
        val placer = subPlacers[placerIdx]
        return placer.getNextTask(host)
    }

    override fun offerTask(task: CacheTask) {
        val placerIdx = (task.objectId % numSchedulers).toInt()
        val placer = subPlacers[placerIdx]
        placer.offerTask(task)
    }

    fun rebalance() {
        subPlacers.map { it. }
    }

}
