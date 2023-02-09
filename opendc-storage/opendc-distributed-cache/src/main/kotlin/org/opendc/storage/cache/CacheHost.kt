package org.opendc.storage.cache

import ch.supsi.dti.isin.cluster.Node
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import org.apache.commons.collections4.map.LRUMap
import java.time.InstantSource

class CacheHost(
    val numProcessingSlots: Int = 4,
    val numCacheSlots: Int = 100,
    val clock: InstantSource,
    val remoteStorage: RemoteStorage,
    val scheduler: TaskScheduler,
    val metricRecorder: MetricRecorder
) {

    companion object {
        var nextHostId: Int = 0
            get() {
                return field++
            }
    }

    val hostId = nextHostId

//    val cache = LRUMap<Long, Boolean>(numCacheSlots, numCacheSlots)
    val cache = HashMap<Long, Boolean>()

    val freeProcessingSlots = Semaphore(numProcessingSlots)

    suspend fun processTasks(channel: SendChannel<CacheTask>) = coroutineScope {
        while(true) {
            freeProcessingSlots.acquire()
            val task = scheduler.getNextTask(this@CacheHost)
            if (task != null) {
                launch {
                    runTask(task)
                    freeProcessingSlots.release()
                    channel.send(task)
                }
            } else {
                break
            }
        }
    }

    suspend fun runTask(task: CacheTask) = coroutineScope {
        metricRecorder.recordStart(task)

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

        metricRecorder.recordCompletion(task)
    }
}
