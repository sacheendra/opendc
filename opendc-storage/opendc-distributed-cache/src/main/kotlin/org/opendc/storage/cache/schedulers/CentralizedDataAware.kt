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
import java.util.PriorityQueue
import kotlin.math.abs
import kotlin.math.roundToInt
import kotlin.time.Duration

class CentralizedDataAwarePlacer(
    val period: Duration,
    val minMovement: Boolean = false,
    val moveSmallestFirst: Boolean = false
): ObjectPlacer {

    override lateinit var scheduler: TaskScheduler
    override var autoscaler: Autoscaler? = null

    val keyToNodeMap = mutableMapOf<Long, CacheHost>()
    val nodeToKeysMap = mutableMapOf<CacheHost, MutableList<Long>>()
    val globalQueue = ChannelQueue(null)

    var complete = false
    val thisFlow = flow<Unit> {
        delay(period)
        while (!complete) {
            rebalance(null)
            emit(Unit)
            delay(period)
        }
    }

    var lateBindingSizeCorrection = 0
    fun globalQueueSize(): Int {
        val ogSize = globalQueue.q.size
        return ogSize - lateBindingSizeCorrection
    }

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
        for(host in hosts) {
            nodeToKeysMap[host] = mutableListOf()
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
            select {
                queue.onReceive {
                    task = queue.next()
                    task
                }
                globalQueue.onReceive {
                    globalTask = globalQueue.next()
                    globalTask
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
            } else {
                keyToNodeMap[globalTask!!.objectId] = host
            }
            return globalTask
        }

        return null
    }

    override fun offerTask(task: CacheTask) {
        val host = getNode(task.objectId)
        if (host != null) {
            val chosenHostId = host.hostId
            val queue = scheduler.hostQueues[chosenHostId]!!

            task.hostId = -1
            queue.add(task)
        }
        globalQueue.add(task)
    }

    fun rebalance(targetScorePerHostInp: Map<CacheHost, Double>?) {
        val perKeyScore = mutableMapOf<Long, Long>()
        var totalKeyScore: Double = 0.0
        // Only considering not yet scheduled tasks
        globalQueue.q.filter { it.hostId == -1 }.forEach {
            val currentScore = perKeyScore.getOrDefault(it.objectId, 0)
            perKeyScore[it.objectId] = currentScore + it.duration
            totalKeyScore += it.duration
        }
        val normalizedPerKeyScore = perKeyScore.mapValues {
            it.value / totalKeyScore
        }
        /*
            Currently moving heaviest objects first, try moving lightest objects first
         */
        val sortedKeys: List<KeyScorePair> = normalizedPerKeyScore.toList().map { KeyScorePair(it.first, it.second) }.sortedByDescending { it.score }
        val targetScorePerHost = if (targetScorePerHostInp == null) {
            val avgScorePerHost = 1.0 / scheduler.hosts.size
            scheduler.hosts.associateWith { avgScorePerHost }
        } else {
            targetScorePerHostInp
        }

        val (keysToAllocate, perHostScores) = if (minMovement) {

            val perHostKeys: Map<CacheHost?, KeyListScorePair> = sortedKeys.groupBy { keyToNodeMap[it.objectId] }.mapValues { entry -> KeyListScorePair(entry.value, entry.value.sumOf { it.score }) }
            val unallocatedKeys = perHostKeys.getOrDefault(null, KeyListScorePair(listOf(), 0.0))
            val allocatedPerHost = perHostKeys.filter { it.key != null }
            val sortedHosts = allocatedPerHost.toList().sortedByDescending { it.second.score - targetScorePerHost[it.first]!! }

            // Trim all keys over average score per host
            val removedKeys = mutableListOf<KeyScorePair>()
            val finalHostsWithScores = sortedHosts.associate { entry ->
                var currentScore = entry.second.score
                val keyList = if (moveSmallestFirst) {
                    // Keys in descending score order
                    entry.second.keyList.toMutableList()
                } else {
                    entry.second.keyList.reversed().toMutableList()
                }

                val targetScore = targetScorePerHost[entry.first]!!
                while (keyList.size > 0) {
                    val lastKey = keyList.removeLast()
                    val newScore = currentScore - lastKey.score
                    if (newScore < targetScore) break

                    removedKeys.add(lastKey)
                    currentScore = newScore
                }
                entry.first!! to currentScore
            }.toMutableMap()

            Pair(unallocatedKeys.keyList + removedKeys, finalHostsWithScores)
        } else {
            Pair(sortedKeys, scheduler.hosts.associateWith { 0.0 }.toMutableMap())
        }

        val hostMinHeap: PriorityQueue<Pair<CacheHost, Double>> = PriorityQueue { a, b -> (a.second - b.second).roundToInt() }
        hostMinHeap.addAll(perHostScores.toList())

        // Assign keys with the highest score first
        for (e in keysToAllocate.sortedByDescending { it.score }) {
            val minHostPair = hostMinHeap.poll()
            val newPair = minHostPair.copy(second = minHostPair.second+e.score)
            hostMinHeap.add(newPair)
            keyToNodeMap[e.objectId] = minHostPair.first
        }
    }

}

data class KeyScorePair(
    val objectId: Long,
    val score: Double
)
typealias KeyList = List<KeyScorePair>
data class KeyListScorePair(
    val keyList: KeyList,
    val score: Double
)
