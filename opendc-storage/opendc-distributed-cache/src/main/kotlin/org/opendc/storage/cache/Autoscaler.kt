package org.opendc.storage.cache

import kotlinx.coroutines.Job
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.cancellable
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import java.time.InstantSource
import java.time.Period
import kotlin.math.ceil
import kotlin.time.Duration

class Autoscaler(val period: Duration,
                 val clock: InstantSource,
                 val remoteStorage: RemoteStorage,
                 val scheduler: TaskScheduler,
                 val metricRecorder: MetricRecorder) {

    suspend fun autoscale() {
        val serviceRate = currentServiceRate().toDouble()
        val submitRate = currentSubmitRate().toDouble()
        val potentialRate = potentialServiceRate().toDouble()

//        val incomingRatio = submitRate / maxOf(potentialRate, 0.1)
        val incomingRatio = submitRate / maxOf(serviceRate, 0.1)
//        println("${incomingRatio} ${submitRate} ${serviceRate}")
        var newPotentialRateDelta = 0.0
        // Is more than 90% of capacity being used
        if (incomingRatio > 0.9) {
            // scale up
            // With an incoming ratio of 0.9, this gives us a new minimum service rate of 1.1*serviceRate
//            newPotentialRateDelta = submitRate - potentialRate + (0.2*submitRate)
            newPotentialRateDelta = submitRate - serviceRate + (0.2*submitRate)
        } else {
            // is less than 70% of capacity being used
            val usedFraction = submitRate / potentialRate
            if (usedFraction < 0.7) {
                // scale down
                newPotentialRateDelta = -(0.8*potentialRate - submitRate)
            }
        }
        val serverChange = ceil(newPotentialRateDelta/serviceRatePerServer).toInt()
        changeNumServers(serverChange)
    }

    suspend fun changeNumServers(serverChange: Int) {
        if (serverChange > 0) {
            scheduler.addHosts((1..serverChange)
                .map { CacheHost(4, 100, clock, remoteStorage, scheduler) })
        } else if (serverChange < 0) {
            if (scheduler.hosts.size == 1) {
                return
            }

            val hostsToRemove = scheduler.hosts.shuffled().take(-serverChange)
            scheduler.removeHosts(hostsToRemove)
        }
    }

    val serviceRatePerServer = 4

    fun potentialServiceRate(): Long {
        return scheduler.hosts.size.toLong() * serviceRatePerServer
    }

    fun currentSubmitRate(): Long {
        return metricRecorder.submittedTaskDurations.sum() / period.inWholeMilliseconds
    }

    fun currentServiceRate(): Long {
        return metricRecorder.completedTaskDurations.sum() / period.inWholeMilliseconds
    }
}
