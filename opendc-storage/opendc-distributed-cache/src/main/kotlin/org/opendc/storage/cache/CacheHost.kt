package org.opendc.storage.cache

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.apache.commons.collections4.map.LRUMap
import java.time.InstantSource

class CacheHost(
    val numProcessingSlots: Int = 4,
    val numCacheSlots: Int = 100,
    val clock: InstantSource,
    val remoteStorage: RemoteStorage,
    val scheduler: TaskScheduler,
) {

    companion object {
        var nextHostId: Int = 0
            get() {
                return field++
            }
    }

    val hostId = nextHostId

    val cache = LRUMap<Long, Boolean>(numCacheSlots, numCacheSlots)

    var freeProcessingSlots = numProcessingSlots

    suspend fun scheduleNextTask() {
        val nextTask = scheduler.getNextTask(this)
        if (nextTask != null) {
            runTask(nextTask)
        }
    }

    suspend fun runTask(task: CacheTask) = coroutineScope {
        task.startTime = clock.millis()
        freeProcessingSlots -= 1
        var storageDelay = 0L
        val objInCache = cache[task.objectId]
        if (objInCache == null) {
            storageDelay = remoteStorage.retrieve()
            cache[task.objectId] = true
            task.isHit = false
        }
        delay(task.duration + storageDelay)
        task.endTime = clock.millis()
        task.storageDelay = storageDelay
        freeProcessingSlots += 1

        launch {
            scheduleNextTask()
        }
    }
}

data class CacheTask(
    val taskId: Long,
    val objectId: Long,
    val duration: Long,
    val submitTime: Long,
    var startTime: Long,
    var endTime: Long,
    var isHit: Boolean = true,
    var hostId: Int,
    var storageDelay: Long,
)
