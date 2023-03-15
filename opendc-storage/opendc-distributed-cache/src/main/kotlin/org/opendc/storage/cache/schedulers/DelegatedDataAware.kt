package org.opendc.storage.cache.schedulers

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.selects.select
import org.opendc.storage.cache.CacheHost
import org.opendc.storage.cache.CacheTask
import org.opendc.storage.cache.TaskScheduler
import java.time.InstantSource
import java.util.PriorityQueue
import kotlin.math.roundToInt
import kotlin.time.Duration

class DelegatedDataAwarePlacer(
    val period: Duration,
    val clock: InstantSource,
    val subPlacers: List<CentralizedDataAwarePlacer>,
    val moveSmallestFirst: Boolean = false,
    val moveOnSteal: Boolean = false,
    val lookBackward: Boolean = false, // to implement
    val minimizeSpread: Boolean = false, // to implement
): ObjectPlacer {

    override lateinit var scheduler: TaskScheduler

    val numPlacers = subPlacers.size
    val hostList: MutableList<CacheHost> = mutableListOf()

    var complete = false
    val thisFlow = flow<TimeCountPair> {

        delay(period)
        while (!complete) {
            val movedCount = rebalance()
            emit(TimeCountPair(clock.millis(), movedCount))
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

    override fun getPlacerFlow(): Flow<TimeCountPair> {
        val subPlacerFlows = subPlacers.map { it.getPlacerFlow() }
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

        var globalTask = false

        var task = queue.next()
        // Late binding check
        while (task != null && task.hostId > 0) {
            task = queue.next()
        }

        var busiestPlacer: CentralizedDataAwarePlacer? = null
        if (task == null) {
            busiestPlacer = subPlacers.shuffled().take(2).maxBy { it.globalQueueSize() }
            task = busiestPlacer.globalQueue.next()
            // Late binding check
            while (task != null && task.hostId > 0) {
                task = busiestPlacer.globalQueue.next()
            }

            if (task != null) globalTask = true
        }

        if (task == null) {
            task = select<CacheTask?> {
                queue.selectWait {
                    queue.next()
                }
                busiestPlacer!!.globalQueue.selectWait {
                    globalTask = true
                    busiestPlacer.globalQueue.next()
                }
            }
        }

        if (task != null && !globalTask) {
            task.hostId = host.hostId
            task.callback?.invoke()
            return task
        }

        if (task != null && globalTask) {
            task.hostId = host.hostId
            if (task.objectId in busiestPlacer!!.keyToNodeMap) {
                task.stolen = true
                if (moveOnSteal) {
                    busiestPlacer.mapKey(task.objectId, host)
                }
            } else {
                busiestPlacer.mapKey(task.objectId, host)
            }
            return task
        }

        return null
    }

    override suspend fun offerTask(task: CacheTask) {
        val placerIdx = (task.objectId % numPlacers).toInt()
        val placer = subPlacers[placerIdx]
        task.callback = {
            placer.lateBindingSizeCorrection++
        }
        placer.offerTask(task)
    }

    fun rebalance(): Int {
        var movedCount = 0
        val choppedParts = 100
        // Calling getPerNodeScores is important to reset counters
        val perPlacerNodeScores = subPlacers.mapIndexed { idx, scores -> IndexedValue(idx, scores.getPerNodeScores()) }

        val totalScore = perPlacerNodeScores.sumOf { it.value.values.sum() }.toDouble()
        val scorePerNode = totalScore / scheduler.hosts.size

//        println("placer counts")
//        println(perPlacerNodeScores)

        val nodeToPlacer: MutableMap<Int?, MutableList<PlacerScorePair>> = mutableMapOf()
        perPlacerNodeScores.forEach { placer ->
            placer.value.forEach { entry ->
                val placerList = nodeToPlacer.computeIfAbsent(entry.key) { _ -> mutableListOf() }
                (0 until choppedParts).forEach { _ ->
                    placerList.add(PlacerScorePair(placer.index, entry.value.toDouble() / choppedParts))
                }
            }
        }

        val unallocatedClaims = nodeToPlacer.getOrDefault(null, mutableListOf())
        @Suppress("UNCHECKED_CAST")
        val requestedClaims: Map<Int, List<PlacerScorePair>> = nodeToPlacer.filter { it.key != null } as Map<Int, List<PlacerScorePair>>

        // Trim allocations to average score per node
        val allocatedClaims = scheduler.hosts.map { host ->
            val rawPlacerList = requestedClaims[host.hostId]
            if (rawPlacerList == null) {
                return@map host.hostId to PlacerListScorePair(mutableListOf(), 0.0)
            }

            // First values are retained, later values moves
            // Hence, largest value first to move small values
            val placerList = if (moveSmallestFirst) {
                rawPlacerList.sortedByDescending { it.score }
            } else {
                rawPlacerList.sortedBy { it.score }
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

            host.hostId to PlacerListScorePair(toReturn.toMutableList(), currentScore)
        }

        val hostMinHeap: PriorityQueue<Pair<Int, PlacerListScorePair>> = PriorityQueue { a, b -> (a.second.score - b.second.score).roundToInt() }
        hostMinHeap.addAll(allocatedClaims)

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
                val hostMap = placerToNodeAllocs.computeIfAbsent(pspair.placer) { _ -> mutableMapOf() }
                val hostScore = hostMap.getOrDefault(hostIdx, 0.0)
                hostMap[hostIdx] = hostScore + pspair.score
            }
        }

        for (placer in subPlacers.withIndex()) {
            // Need to normalize as each subplacer expects allocations that sum to 1
            val nodeAllocs = placerToNodeAllocs[placer.index]
            if (nodeAllocs != null) {
                val placerScore = nodeAllocs.values.sum()
                val normalizedAllocs = nodeAllocs.mapValues { it.value / placerScore }
                movedCount += placer.value.rebalance(normalizedAllocs)
            }
        }

        return movedCount
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
