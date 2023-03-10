package org.opendc.storage.cache.schedulers

import ch.supsi.dti.isin.consistenthash.ConsistentHash
import kotlinx.coroutines.flow.Flow
import org.opendc.storage.cache.Autoscaler
import org.opendc.storage.cache.CacheHost
import org.opendc.storage.cache.CacheTask
import org.opendc.storage.cache.TaskScheduler

class ConsistentHashWrapper(
    val chash: ConsistentHash,
    val stealWork: Boolean = false
): ObjectPlacer {

    override lateinit var scheduler: TaskScheduler
    fun getNode(key: Long): CacheHost {
        return chash.getNode(key.toString()) as CacheHost
    }

    override fun addHosts(hosts: List<CacheHost>) {
        chash.addNodes(hosts)
    }

    override fun removeHosts(hosts: List<CacheHost>) {
        chash.removeNodes(hosts)
    }

    override fun getPlacerFlow(): Flow<Unit>? {
        return null
    }

    override suspend fun complete() {}
    override suspend fun getNextTask(host: CacheHost): CacheTask? {
        val queue = scheduler.hostQueues[host.hostId]
        if (queue == null) return null // This means the node has been deleted

        var task = queue.next()

        if (task == null) {
            if (stealWork) {
                // Work steal
                val chosenQueue = scheduler.hostQueues.values
                    .maxWith{a, b -> a.q.size - b.q.size}
                if (chosenQueue.q.size > 5) {
                    val globalTask = chosenQueue.next()!!
                    globalTask.stolen = true
                    globalTask.hostId = globalTask.hostId
                    return globalTask
                }
            }

            queue.wait()
            task = queue.next()
        }

        task?.hostId = host.hostId

        return task
    }

    override suspend fun offerTask(task: CacheTask) {
        // Decide host
        val host = getNode(task.objectId)
        val chosenHostId = host.hostId
        val queue = scheduler.hostQueues[chosenHostId]!!

        queue.add(task)
    }

}
