package org.opendc.storage.cache

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.sync.Semaphore
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

    val freeProcessingSlots = Semaphore(numProcessingSlots)

    val completedTaskFlow: Flow<CacheTask>

    init {
        completedTaskFlow = flow {
            var task = scheduler.getNextTask(this@CacheHost)
            while (task != null) {
                freeProcessingSlots.acquire()
                runTask(task)
                freeProcessingSlots.release()
                emit(task)
                task = scheduler.getNextTask(this@CacheHost)
            }
        }.buffer() // Buffer is necessary.
        // Or the flow code will be called each time its consumed.
    }

    suspend fun runTask(task: CacheTask) = coroutineScope {
        task.startTime = clock.millis()
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
    }
}
