package org.opendc.storage.cache

import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Semaphore
import org.apache.commons.math3.random.*

class RemoteStorage(
    var concurrentTasks: Int = 8
) {

    var freeProcessingSlots = Semaphore(concurrentTasks)
    val dist: EmpiricalDistribution = EmpiricalDistribution(7)

    init {
        val rawSamples = (
            DoubleArray(10) {3556.0}
                + DoubleArray(10) {2884.0}
                + DoubleArray(100) {2280.0}
                + DoubleArray(1000) {1256.0}
                + DoubleArray(4000) {100.0}
                + DoubleArray(5000) {45.0}
                + DoubleArray(90000) {21.0}
//                + DoubleArray(4000) {45.0}
//                + DoubleArray(5000) {21.0}
//                + DoubleArray(90000) {13.0}
            )
        dist.load(rawSamples)
    }

    suspend fun retrieve(): Long {
//        return dist.sample().toLong()
        freeProcessingSlots.acquire()
        freeProcessingSlots.release()
        return 0
    }

    fun updateConcurrentSlots(newConcurrency: Int) {
        val oldConcurrency = concurrentTasks
        concurrentTasks = newConcurrency
        freeProcessingSlots = Semaphore(newConcurrency, oldConcurrency - freeProcessingSlots.availablePermits)
    }
}
