package org.opendc.storage.cache.schedulers

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.selects.select
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
    val moveSmallestFirstUnderlying: Boolean = false,
    val moveOnSteal: Boolean = false,
    val lookBackwardUnderlying: Boolean = false,
    val moveSmallestFirst: Boolean = true,
    val lookBackward: Boolean = false, // to implement
    val minimizeSpread: Boolean = false, // to implement
): ObjectPlacer {

    override lateinit var scheduler: TaskScheduler
    override var autoscaler: Autoscaler? = null

    val subPlacers: List<CentralizedDataAwarePlacer> = List(numSchedulers) { _ -> CentralizedDataAwarePlacer(period, minMovement, moveSmallestFirstUnderlying, moveOnSteal, lookBackwardUnderlying) }
    val hostList: MutableList<CacheHost> = mutableListOf()

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
        hostList.addAll(hosts)
        for (placer in subPlacers) {
            placer.addHosts(hosts)
        }
        rebalance()
    }

    override fun removeHosts(hosts: List<CacheHost>) {
        hostList.removeAll(hosts)
        for (placer in subPlacers) {
            placer.removeHosts(hosts)
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
        val queue = scheduler.hostQueues[host.hostId]
        if (queue == null) return null // This means the node has been deleted

        if (queue.closed) return null

        var task = queue.next()
        // Late binding check
        while (task != null && task.hostId > 0) {
            task = queue.next()
        }

        var globalTask: CacheTask? = null
        var busiestPlacer: CentralizedDataAwarePlacer? = null
        if (task == null) {
            busiestPlacer = subPlacers.shuffled().subList(0, 2).maxBy { it.globalQueueSize() }
            globalTask = busiestPlacer.globalQueue.next()
        }

        if (task == null && globalTask == null) {
            select {
                queue.onReceive {
                    task = queue.next()
                    task
                }
                busiestPlacer!!.globalQueue.onReceive {
                    globalTask = busiestPlacer.globalQueue.next()
                    globalTask
                }
            }
        }

        if (task != null) {
            task!!.hostId = host.hostId
            return task
        }

        if (globalTask != null) {
            globalTask!!.hostId = host.hostId
            if (globalTask!!.objectId in busiestPlacer!!.keyToNodeMap) {
                globalTask!!.stolen = true
            } else {
                busiestPlacer.keyToNodeMap[globalTask!!.objectId] = host
            }
            return globalTask
        }

        return null
    }

    override fun offerTask(task: CacheTask) {
        val placerIdx = (task.objectId % numSchedulers).toInt()
        val placer = subPlacers[placerIdx]
        placer.offerTask(task)
    }

    fun rebalance() {
        val totalScore = subPlacers.sumOf { it.perNodeScore.values.sum() }.toDouble()
        val scorePerNode = totalScore / scheduler.hosts.size

        val nodeToScore: MutableMap<Int?, List<PlacerScorePair>> = mutableMapOf()
        subPlacers.withIndex().forEach { placer ->
            placer.value.perNodeScore.forEach { entry ->
                nodeToScore.merge(entry.key, listOf(PlacerScorePair(
                    placer.index, entry.value
                ))) {x, y -> x+y}
            }
        }
        val withSortedPlacers = nodeToScore.map { entry ->
            entry.key to entry.value.sortedByDescending { it.score }
        }.toMap()
        val newAllocs = withSortedPlacers[null]
        val requestedAllocs: Map<Int, List<PlacerScorePair>> = withSortedPlacers.filter { it.key != null } as Map<Int, List<PlacerScorePair>>

        requestedAllocs.forEach()

    }

}

data class PlacerScorePair(
    val placer: Int,
    val score: Int
)
