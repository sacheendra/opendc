package org.opendc.storage.cache.schedulers

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.selects.select
import org.opendc.storage.cache.CacheHost
import org.opendc.storage.cache.CacheTask
import org.opendc.storage.cache.TaskScheduler
import java.util.PriorityQueue
import kotlin.math.roundToInt
import kotlin.time.Duration

class DelegatedDataAwarePlacer(
    val period: Duration,
    val subPlacers: List<CentralizedDataAwarePlacer>,
    val moveSmallestFirst: Boolean = false,
    val lookBackward: Boolean = false, // to implement
    val minimizeSpread: Boolean = false, // to implement
): ObjectPlacer {

    override lateinit var scheduler: TaskScheduler

    val numSchedulers = subPlacers.size
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

    override fun registerScheduler(scheduler: TaskScheduler) {
        this.scheduler = scheduler
        subPlacers.forEach {
            it.registerScheduler(scheduler)
        }
    }

    override fun getPlacerFlow(): Flow<Unit> {
        val subPlacerFlows = subPlacers.mapNotNull { it.getPlacerFlow() }
        return subPlacerFlows.plus(thisFlow).merge()
    }

    override suspend fun complete() {
        complete = true
        subPlacers.forEach {
            it.complete()
        }
    }

    override suspend fun getNextTask(host: CacheHost): CacheTask? {
        val queue = scheduler.hostQueues[host.hostId]
        if (queue == null) return null // This means the node has been deleted

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
            select<Unit> {
                queue.selectWait {
                    task = queue.next()
                }
                busiestPlacer!!.globalQueue.selectWait {
                    globalTask = busiestPlacer!!.globalQueue.next()
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

    override suspend fun offerTask(task: CacheTask) {
        val placerIdx = (task.objectId % numSchedulers).toInt()
        val placer = subPlacers[placerIdx]
        placer.offerTask(task)
    }

    fun rebalance() {
        val choppedParts = 100
        // Calling getPerNodeScores is important to reset counters
        val perPlacerNodeScores = subPlacers.mapIndexed { idx, scores -> IndexedValue(idx, scores.getPerNodeScores()) }

        val totalScore = perPlacerNodeScores.sumOf { it.value.values.sum() }.toDouble()
        val scorePerNode = totalScore / scheduler.hosts.size

        val nodeToPlacer: MutableMap<Int?, MutableList<PlacerScorePair>> = mutableMapOf()
        perPlacerNodeScores.forEach { placer ->
            placer.value.forEach { entry ->
                val placerList = nodeToPlacer.getOrDefault(entry.key, mutableListOf())
                (0 until choppedParts).forEach { _ ->
                    placerList.add(PlacerScorePair(placer.index, entry.value.toDouble() / choppedParts))
                }
            }
        }

        val unallocatedClaims = nodeToPlacer.getOrDefault(null, listOf()).toMutableList()
        val requestedClaims: Map<Int, List<PlacerScorePair>> = nodeToPlacer.filter { it.key != null } as Map<Int, List<PlacerScorePair>>

        // Trim allocations to average score per node
        val allocatedClaims = requestedClaims.mapValues { entry ->
            // First values are retained, later values moves
            // Hence, largest value first to move small values
            val placerList = if (moveSmallestFirst) {
                entry.value.sortedByDescending { it.score }
            } else {
                entry.value.sortedBy { it.score }
            }

            var currentScore = 0.0
            var cutOffIndex = 0
            for (it in placerList.withIndex()) {
                val newScore = currentScore + it.value.score
                if (newScore > scorePerNode) break

                currentScore = newScore
                cutOffIndex = it.index
            }

            val toReturn = placerList.subList(0, cutOffIndex+1)
            unallocatedClaims.addAll(placerList.subList(cutOffIndex+1, placerList.size))

            PlacerListScorePair(toReturn.toMutableList(), currentScore)
        }

        val hostMinHeap: PriorityQueue<Pair<Int, PlacerListScorePair>> = PriorityQueue { a, b -> (a.second.score - b.second.score).roundToInt() }
        hostMinHeap.addAll(allocatedClaims.toList())

        for (e in unallocatedClaims.sortedByDescending { it.score }) {
            val minHostPair = hostMinHeap.poll()
            minHostPair.second.score += e.score
            hostMinHeap.add(minHostPair)
        }

        // Need to normalize before returning to hosts
        val placerToNodeAllocs: MutableMap<Int, MutableMap<Int, Double>> = mutableMapOf()
        hostMinHeap.forEach {
            val hostIdx = it.first
            val placerList = it.second.placerList
            placerList.forEach { pspair ->
                val hostMap = placerToNodeAllocs.getOrDefault(pspair.placer, mutableMapOf())
                val hostScore = hostMap.getOrDefault(hostIdx, 0.0)
                hostMap[hostIdx] = hostScore + pspair.score
            }
        }

        for (placer in subPlacers.withIndex()) {
            placer.value.rebalance(placerToNodeAllocs[placer.index])
        }
    }

}

data class PlacerScorePair(
    val placer: Int,
    val score: Double
)

data class PlacerListScorePair(
    val placerList: MutableList<PlacerScorePair>,
    var score: Double
)
