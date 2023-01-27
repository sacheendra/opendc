package org.opendc.storage.cache.schedulers

import ch.supsi.dti.isin.cluster.Node
import ch.supsi.dti.isin.consistenthash.ConsistentHash
import org.opendc.storage.cache.TaskScheduler

interface DynamicObjectPlacer {
    fun registerScheduler(scheduler: TaskScheduler)
}

class GreedyObjectPlacer: ConsistentHash, DynamicObjectPlacer {

    lateinit var scheduler: TaskScheduler
    override fun getNode(key: String?): Node {
        val shortestQueue = scheduler.hostQueues.values
            .minBy { it.size }
        return shortestQueue.host
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

class RandomObjectPlacer: ConsistentHash, DynamicObjectPlacer {

    lateinit var scheduler: TaskScheduler
    override fun getNode(key: String?): Node {
        return scheduler.hosts.random()
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
