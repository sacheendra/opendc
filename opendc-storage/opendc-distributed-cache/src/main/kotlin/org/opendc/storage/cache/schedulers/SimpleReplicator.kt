package org.opendc.storage.cache.schedulers

import ch.supsi.dti.isin.cluster.Node
import ch.supsi.dti.isin.consistenthash.ConsistentHash
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach
import org.opendc.storage.cache.CacheHost
import org.opendc.storage.cache.ChannelQueue
import org.opendc.storage.cache.TaskScheduler
import kotlin.time.Duration

interface Replicator {
    fun getNodes(key: String): Array<Node>
}

class SimpleReplicator(val chash: ConsistentHash,
                       val period: Duration
): ConsistentHash, DynamicObjectPlacer, Replicator {

    lateinit var scheduler: TaskScheduler
    val counts = HashMap<String, Int>()
    var popularKeys: Set<String> = HashSet()
    var complete = false

    val replicatorFlow: Flow<Unit> = flow<Unit> {
        delay(period)
        while (!complete) {
            popularKeys = counts.toList().sortedByDescending { (_, v) -> v }.take(1000).map { it.first }.toSet()
            counts.clear()
            delay(period)
        }
    }

    override fun getNode(key: String?): Node {
        return chash.getNode(key)
    }

    override fun getNodes(key: String): Array<Node> {
        val currentValue = counts.get(key)
        counts[key] = if (currentValue == null) 1 else currentValue+1

        val primaryQueue = chash.getNode(key) as ChannelQueue

        return if (popularKeys.contains(key)) {
            val secondaryQueue = scheduler.queues[(primaryQueue.idx + 1) % scheduler.queues.size]
            arrayOf(primaryQueue, secondaryQueue)
        } else {
            arrayOf(primaryQueue)
        }
    }

    override fun addNodes(nodes: MutableCollection<out Node>?) {
        return
    }

    override fun removeNodes(nodes: MutableCollection<out Node>?) {
        return
    }

    override fun nodeCount(): Int {
        return 0
    }

    override fun engine(): Any? {
        return null
    }

    override fun registerScheduler(scheduler: TaskScheduler) {
        this.scheduler = scheduler
    }

}
