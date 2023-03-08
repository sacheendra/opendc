package org.opendc.storage.cache.schedulers

import kotlinx.coroutines.flow.Flow
import org.opendc.storage.cache.Autoscaler
import org.opendc.storage.cache.CacheHost
import org.opendc.storage.cache.CacheTask
import org.opendc.storage.cache.TaskScheduler

interface ObjectPlacer {

    fun addHosts(hosts: List<CacheHost>)

    fun removeHosts(hosts: List<CacheHost>)

    var scheduler: TaskScheduler
    fun registerScheduler(scheduler: TaskScheduler) {
        this.scheduler = scheduler
    }

    var autoscaler: Autoscaler?
    fun registerAutoscaler(autoscaler: Autoscaler?) {
        this.autoscaler = autoscaler
    }

    fun getPlacerFlow(): Flow<Unit>?

    suspend fun complete()

    suspend fun getNextTask(host: CacheHost): CacheTask?

    suspend fun offerTask(task: CacheTask)
}
