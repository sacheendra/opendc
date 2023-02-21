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

class CentralizedDataAwarePlacer(
    val period: Duration,
    val minMovement: Boolean = false
): ObjectPlacer {

    override lateinit var scheduler: TaskScheduler
    override var autoscaler: Autoscaler? = null

    val keyToNodeMap = mutableMapOf<Long, CacheHost>()
    val nodeToKeysMap = mutableMapOf<CacheHost, MutableList<Long>>()
    val globalQueue = ChannelQueue(null)
//    val hostQueueHeap: PriorityQueue<ChannelQueue> = PriorityQueue { a, b -> a.q.size - b.q.size }

    var complete = false
    val thisFlow = flow<Unit> {
        delay(period)
        while (!complete) {
            rebalance()
            emit(Unit)
            delay(period)
        }
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
        }

        var globalTask: CacheTask? = null
        if (task == null) {
            // This is used both for new tasks and for workstealing
            globalTask = globalQueue.next()
            // Late binding check
            while (globalTask != null && globalTask.hostId > 0) {
                globalTask = globalQueue.next()
            }

            if (globalTask == null) {
                queue.wait()
                task = queue.next()
            }
        }

        if (task != null) {
            task.hostId = host.hostId
            return task
        }

        if (globalTask != null) {
            globalTask.hostId = host.hostId
            if (globalTask.objectId in keyToNodeMap) {
                globalTask.stolen = true
            } else {
                keyToNodeMap[globalTask.objectId] = host
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
            queue.q.add(task)
            queue.pleaseNotify()
        }
        globalQueue.q.add(task)
    }

    suspend fun rebalance() {
        val perKeyScore = mutableMapOf<Long, Long>()
        // Only considering not yet scheduled tasks
        globalQueue.q.filter { it.hostId == -1 }.forEach {
            val currentScore = perKeyScore.getOrDefault(it.objectId, 0)
            perKeyScore[it.objectId] = currentScore + it.duration
        }
        /*
            Currently moving heaviest objects first, try moving lightest objects first
         */
        val sortedKeys: List<KeyScorePair> = perKeyScore.toList().map { KeyScorePair(it.first, it.second) }.sortedByDescending { it.score }

        if (autoscaler != null) {
            TODO("Make a plausible autoscaler")
            val totalDuration = sortedKeys.sumOf { it.score }
            val occupancy = totalDuration / (scheduler.hosts.size * period.inWholeMilliseconds)

            if (occupancy > 1 || occupancy < 0.6) {
                val newNumHosts = (totalDuration / (0.8 * period.inWholeMilliseconds)).toInt()
                val hostsChange = newNumHosts - scheduler.hosts.size
                autoscaler!!.changeNumServers(hostsChange)
            }
        }

        val (keysToAllocate, perHostScores) = if (minMovement) {
            val totalScore = sortedKeys.sumOf { it.score }
            val avgScorePerHost = totalScore / scheduler.hosts.size

            val perHostKeys: Map<CacheHost?, KeyListScorePair> = sortedKeys.groupBy { keyToNodeMap[it.objectId] }.mapValues { entry -> KeyListScorePair(entry.value, entry.value.sumOf { it.score }) }
            val unallocatedKeys = perHostKeys.getOrDefault(null, KeyListScorePair(listOf(), 0))
            val allocatedPerHost = perHostKeys.filter { it.key != null }
            val sortedHosts = allocatedPerHost.toList().sortedByDescending { it.second.score }

            // Trim all keys over average score per host
            val removedKeys = mutableListOf<KeyScorePair>()
            val finalHostsWithScores = sortedHosts.map { entry ->
                var currentScore = entry.second.score
                val ascendingKeyList = entry.second.keyList.reversed().toMutableList()
                while (ascendingKeyList.size > 0) {
                    val lastKey = ascendingKeyList.removeLast()
                    val newScore = currentScore - lastKey.score
                    if (newScore < avgScorePerHost) break

                    removedKeys.add(lastKey)
                    currentScore = newScore
                }
                entry.first!! to currentScore
            }.toMap().toMutableMap()

            Pair(unallocatedKeys.keyList + removedKeys, finalHostsWithScores)
        } else {
            Pair(sortedKeys, scheduler.hosts.map { it to 0L }.toMap().toMutableMap())
        }


        for (e in keysToAllocate) {
            val minHost = perHostScores.minBy { it.value }.key
            perHostScores[minHost] = perHostScores[minHost]!! + e.score
            keyToNodeMap[e.objectId] = minHost
        }
    }

}

data class KeyScorePair(
    val objectId: Long,
    val score: Long
)
typealias KeyList = List<KeyScorePair>
data class KeyListScorePair(
    val keyList: KeyList,
    val score: Long
)
