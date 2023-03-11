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
import java.time.InstantSource
import java.util.PriorityQueue
import kotlin.math.roundToInt
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

class CentralizedDataAwarePlacer(
    val period: Duration,
    val clock: InstantSource,
    val minMovement: Boolean = true,
    val stealWork: Boolean = false,
    val moveSmallestFirst: Boolean = false,
    val moveOnSteal: Boolean = false,
    val lookBackward: Boolean = false // to implement
): ObjectPlacer {

    override lateinit var scheduler: TaskScheduler

    val keyToNodeMap = mutableMapOf<Long, HostEnTime>()
    val nodeToKeysMap = mutableMapOf<CacheHost, MutableSet<Long>>()

    fun mapKey(key: Long, host: CacheHost) {
        keyToNodeMap[key] = HostEnTime(host, clock.millis())
        nodeToKeysMap[host]!!.add(key)
    }

    val globalQueue = ChannelQueue(null)

    val perNodeScore = mutableMapOf<Int?, Int>()
    val perKeyScore = mutableMapOf<Long, Int>()

    var complete = false
    val thisFlow = flow<Unit> {
        delay(period)
        while (!complete) {
            rebalance(null)
            getPerNodeScores()
            emit(Unit)
            delay(period)
        }
    }

    var lateBindingSizeCorrection = 0
    fun globalQueueSize(): Int {
        val ogSize = globalQueue.q.size
        return ogSize - lateBindingSizeCorrection
    }

    override fun addHosts(hosts: List<CacheHost>) {
        for(host in hosts) {
            nodeToKeysMap[host] = mutableSetOf()
        }
    }

    override fun removeHosts(hosts: List<CacheHost>) {
        for (host in hosts) {
            for (key in nodeToKeysMap.remove(host)!!) {
                keyToNodeMap.remove(key)
            }
        }
    }

    override fun getPlacerFlow(): Flow<Unit>? {
        return thisFlow
    }

    override suspend fun complete() {
        complete = true
        globalQueue.close()
    }

    override suspend fun getNextTask(host: CacheHost): CacheTask? {
        val queue = scheduler.hostQueues[host.hostId]
        if (queue == null) return null // This means the node has been deleted

        var task = queue.next()
        // Late binding check
        while (task != null && task.hostId > 0) {
            task = queue.next()
            lateBindingSizeCorrection--
        }

        var globalTask: CacheTask? = null
        if (task == null) {
            // This is used both for new tasks and for workstealing
            globalTask = globalQueue.next()
            // Late binding check
            while (globalTask != null && globalTask.hostId > 0) {
                globalTask = globalQueue.next()
            }
        }

        if (task == null && globalTask == null) {
            select<Unit> {
                queue.selectWait {
                    task = queue.next()
                }
                globalQueue.selectWait {
                    globalTask = globalQueue.next()
                }
            }
        }

        if (task != null) {
            lateBindingSizeCorrection++
            task!!.hostId = host.hostId
            return task
        }

        if (globalTask != null) {
            globalTask!!.hostId = host.hostId
            if (globalTask!!.objectId in keyToNodeMap) {
                globalTask!!.stolen = true
                if (moveOnSteal) {
                    mapKey(globalTask!!.objectId, host)
                }
            } else {
                mapKey(globalTask!!.objectId, host)
            }
            return globalTask
        }

        return null
    }

    override suspend fun offerTask(task: CacheTask) {
        task.hostId = -1 // Need to do this as this might be relocated task

        perKeyScore.merge(task.objectId, 1, Int::plus)
        val keyEntry = keyToNodeMap[task.objectId]
        if (keyEntry != null) {
            // Reset every five minutes to support location reset when not rebalancing
            if (clock.millis() - keyEntry.time > 5.minutes.inWholeMilliseconds) {
                keyToNodeMap.remove(task.objectId)
                nodeToKeysMap[keyEntry.host]!!.remove(task.objectId)

                perNodeScore.merge(null, 1, Int::plus)
                globalQueue.add(task)
                return
            }

            val chosenHostId = keyEntry.host.hostId
            val queue = scheduler.hostQueues[chosenHostId]!!

            queue.add(task)
            perNodeScore.merge(chosenHostId, 1, Int::plus)
            if (stealWork) {
                globalQueue.add(task)
            }
        } else {
            perNodeScore.merge(null, 1, Int::plus)
            globalQueue.add(task)
        }
    }

    fun getPerNodeScores(): Map<Int?, Int> {
        val frozen = perNodeScore.toMap()
        perNodeScore.clear()
        return frozen
    }

    var prevTargetScores: Map<Int, Double>? = null
    fun rebalance(targetScorePerHostInp: Map<Int, Double>?) {
        val totalKeyScore = perKeyScore.values.sum().toDouble()
        val normalizedPerKeyScore = perKeyScore.mapValues {
            it.value / totalKeyScore
        }.toList().map { KeyScorePair(it.first, it.second) }.sortedBy { it.score }
        perKeyScore.clear()
        /*
            Currently moving heaviest objects first, try moving lightest objects first
         */
        val targetScorePerHost: Map<Int, Double> = if (targetScorePerHostInp != null) {
            prevTargetScores = targetScorePerHostInp
            targetScorePerHostInp
        } else if(prevTargetScores != null) {
            prevTargetScores as Map<Int, Double>
        } else {
            val avgScorePerHost = 1.0 / scheduler.hosts.size
            scheduler.hosts.map { it.hostId }.associateWith { avgScorePerHost }
        }

        // Groupby preserves order according to the docs
        val perHostKeys: Map<CacheHost?, KeyList> = normalizedPerKeyScore.groupBy { keyToNodeMap[it.objectId]?.host }
        val unallocatedKeys = perHostKeys.getOrDefault(null, listOf())
        @Suppress("UNCHECKED_CAST")
        val allocatedPerHost = perHostKeys.filter { it.key != null } as Map<CacheHost, KeyList>

        val (keysToAllocate, perHostScores) = if (minMovement && allocatedPerHost.isNotEmpty()) {
//            val sortedHosts = allocatedPerHost.toList().sortedByDescending { it.second.score - targetScorePerHost[it.first]!! } // Sorting hosts here. Not the keys in hosts
//println(perHostKeys.keys)
            // Trim all keys over average score per host
            val removedKeys = mutableListOf<KeyScorePair>()
            val finalHostsWithScores = allocatedPerHost.map { entry ->

                // First values are retained, later values moves
                // Hence, largest value first to move small values
                val keyList = if (moveSmallestFirst) {
                    entry.value.reversed()
                } else {
                    entry.value
                }

                val targetScore = targetScorePerHost.getOrDefault(entry.key.hostId, 0.0)
                var currentScore = 0.0
                var cutOffIndex = 0
                for (it in keyList.withIndex()) {
                    val newScore = currentScore + it.value.score
                    if (newScore > targetScore) break

                    currentScore = newScore
                    cutOffIndex = it.index
                }
                removedKeys.addAll(keyList.subList(cutOffIndex+1, keyList.size))

                entry.key to currentScore
            }.toMap()

            Pair(unallocatedKeys + removedKeys, finalHostsWithScores)
        } else {
            Pair(normalizedPerKeyScore, scheduler.hosts.associateWith { 0.0 })
        }

        val hostMinHeap: PriorityQueue<Pair<CacheHost, Double>> = PriorityQueue { a, b -> (a.second - b.second).roundToInt() }
//        println(perHostScores.toList())
        hostMinHeap.addAll(perHostScores.toList())

        // Assign keys with the highest score first
        for (e in keysToAllocate.sortedByDescending { it.score }) {
            val minHostPair = hostMinHeap.poll()
            val newPair = minHostPair.copy(second = minHostPair.second+e.score)
            hostMinHeap.add(newPair)
            mapKey(e.objectId, minHostPair.first)
        }
    }

}

data class KeyScorePair(
    val objectId: Long,
    val score: Double
)
typealias KeyList = List<KeyScorePair>

data class HostEnTime(
    val host: CacheHost,
    val time: Long
)
