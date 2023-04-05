package org.opendc.storage.remote

import org.opendc.storage.cache.CacheHost
import org.opendc.storage.cache.CacheTask
import org.opendc.storage.cache.RemoteStorage
import org.opendc.storage.cache.TaskScheduler
import java.time.Clock

object GlobalScheduler {

    lateinit var sched: TaskScheduler
    lateinit var remoteStorage: RemoteStorage
    val clock = Clock.systemUTC()

    val idToHostMap = mutableMapOf<String, CacheHost>()

    fun initialize(scheduler: TaskScheduler) {
        sched = scheduler
        remoteStorage = RemoteStorage(true)

    }

    suspend fun addHost(hostId: String): String {
        if (idToHostMap.containsKey(hostId)){
            return "ALREADY EXISTS"
        }
        idToHostMap[hostId] = CacheHost(4,1000, clock, remoteStorage, sched)
        sched.addHosts(listOf(idToHostMap[hostId]!!))
        return "DONE"
    }

    suspend fun offerTask(taskId: String, objectId: String, duration: Long, metadata:String) {
        sched.offerTask(CacheTask(taskId.toLong(), objectId.toLong(), duration, clock.millis(), metadata=metadata))
    }

    suspend fun nextTask(hostId: String): String {
        val task = sched.getNextTask(idToHostMap[hostId]!!)
        if (task == null) {
            return ""
        }
        return "${task.taskId},${task.objectId},${task.duration},${task.metadata}"
    }
}
