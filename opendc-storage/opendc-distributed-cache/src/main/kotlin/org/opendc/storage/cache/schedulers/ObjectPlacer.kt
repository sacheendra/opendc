package org.opendc.storage.cache.schedulers

import kotlinx.coroutines.flow.Flow
import org.opendc.storage.cache.CacheHost
import org.opendc.storage.cache.CacheTask
import org.opendc.storage.cache.TaskScheduler

interface ObjectPlacer {
    fun getNode(key: Long): CacheHost

    fun addHosts(hosts: List<CacheHost>)

    fun removeHosts(hosts: List<CacheHost>)

    fun registerScheduler(scheduler: TaskScheduler)

    fun getPlacerFlow(): Flow<Unit>?

    fun complete()

    suspend fun getNextTask(host: CacheHost): CacheTask?
    
    fun offerTask(task: CacheTask)
}
