package org.opendc.storage.cache.schedulers

import ch.supsi.dti.isin.consistenthash.ConsistentHash
import kotlinx.coroutines.flow.Flow
import org.opendc.storage.cache.CacheHost
import org.opendc.storage.cache.TaskScheduler

class ConsistentHashWrapper(val chash: ConsistentHash): ObjectPlacer {
    override fun getNode(key: Long): CacheHost {
        return chash.getNode(key.toString()) as CacheHost
    }

    override fun addHosts(hosts: List<CacheHost>) {
        chash.addNodes(hosts)
    }

    override fun removeHosts(hosts: List<CacheHost>) {
        chash.removeNodes(hosts)
    }

    override fun registerScheduler(scheduler: TaskScheduler) {
        TODO("Not yet implemented")
    }

    override fun getPlacerFlow(): Flow<Unit>? {
        return null
    }

    override fun complete() {}

}
