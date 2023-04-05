package org.opendc.storage.cache

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import java.time.InstantSource

class ManualScaler(
    val triggertime: Long,
    val numHosts: Long,
    val concurrentTasks: Int,
    val cacheSlots: Int,
    val scheduler: TaskScheduler,
    val clock: InstantSource,
    val remoteStorage: RemoteStorage,
    val metricRecorder: MetricRecorder) {

    val manualFlow: Flow<Unit> = flow {
        delay(triggertime)
        val serverChange = (numHosts - scheduler.hosts.size).toInt()

        if (serverChange > 0) {
            scheduler.addHosts((1..serverChange)
                .map { CacheHost(concurrentTasks, cacheSlots, clock, remoteStorage, scheduler) })
        } else if (serverChange < 0) {
            if (scheduler.hosts.size == 1) {
                return@flow
            }

            val hostsToRemove = scheduler.hosts.shuffled().take(-serverChange)
            scheduler.removeHosts(hostsToRemove)
        }
    }
}
