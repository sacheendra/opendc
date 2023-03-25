package org.opendc.storage.cache

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Semaphore
import org.apache.commons.math3.random.*

class RemoteStorage {

    val dist: EmpiricalDistribution = EmpiricalDistribution(7)

    init {
        val rawSamples = (
            DoubleArray(10) {3556.0}
                + DoubleArray(100) {2884.0}
                + DoubleArray(1000) {2280.0}
                + DoubleArray(4000) {1256.0}
                + DoubleArray(5000) {45.0}
                + DoubleArray(40000) {21.0}
                + DoubleArray(50000) {13.0}
            )
//            DoubleArray(10) {3556.0}
//                + DoubleArray(10) {2884.0}
//                + DoubleArray(100) {2280.0}
//                + DoubleArray(1000) {1256.0}
//                + DoubleArray(4000) {100.0}
//                + DoubleArray(5000) {45.0}
//                + DoubleArray(90000) {21.0}
//            )
        dist.load(rawSamples)
    }

    suspend fun retrieve(duration: Long): Long {
        // If task needs 100mbps
        // Remote only offers 80mbps
        // Additional delay = ((100-80)/80) * duration
//        val storageDelay = (duration/4.0 + dist.sample()).toLong()

        val storageDelay = dist.sample().toLong()
        return storageDelay
    }
}
