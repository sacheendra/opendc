package org.opendc.storage.cache

import ch.supsi.dti.isin.cluster.Node
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import org.apache.commons.collections4.map.LRUMap
import org.opendc.storage.cache.schedulers.GreedyObjectPlacer
import java.time.InstantSource

class CacheHost(
    val concurrentTasks: Int = 4,
    val numCacheSlots: Int = 1000,
    val clock: InstantSource,
    val remoteStorage: RemoteStorage,
    val scheduler: TaskScheduler,
    val metricRecorder: MetricRecorder
) : Node {

    companion object {
        var nextHostId: Int = 0
            get() {
                return field++
            }
    }

    val hostId = nextHostId

    val cache: MutableMap<Long, Boolean> = if (numCacheSlots > 0) {
        LRUMap<Long, Boolean>(numCacheSlots, numCacheSlots)
    } else {
        HashMap<Long, Boolean>()
    }

    val freeProcessingSlots = Semaphore(concurrentTasks)

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

    override fun name(): String {
        return hostId.toString()
    }

    override fun compareTo(other: Node?): Int {
        other as CacheHost
        return hostId.compareTo(other.hostId)
    }
}
