package org.opendc.storage.cache

import java.time.InstantSource
import kotlin.math.ceil
import kotlin.time.Duration

class Autoscaler(val period: Duration,
                 val clock: InstantSource,
                 val remoteStorage: RemoteStorage,
                 val scheduler: TaskScheduler,
                 val metricRecorder: MetricRecorder,
    val watermarks: Pair<Double, Double>
) {


    val lowWatermark = watermarks.first
    val highWatermark = watermarks.second

    suspend fun autoscale() {
        val serviceRate = currentServiceRate()
        val submitRate = currentSubmitRate()
        val potentialRate = potentialServiceRate().toDouble()

//        println("$serviceRate $submitRate")

        // Actual service before the first epoch will be low as few tasks were completed
        val rateToCompare = if (clock.millis() < 1000*60*2) {
            potentialRate
        } else {
            serviceRate
        }
        val incomingRatio = submitRate / maxOf(rateToCompare, 0.1)
//        println("${incomingRatio} ${submitRate} ${serviceRate}")
        var newPotentialRateDelta = 0.0
        // Is more than 90% of capacity being used
        if (incomingRatio > highWatermark) {
            // scale up
            // With an incoming ratio of 0.9, this gives us a new minimum service rate of 1.1*serviceRate
            newPotentialRateDelta = submitRate - serviceRate + (0.2*submitRate)
        } else {
            // is less than 70% of capacity being used
            val usedFraction = submitRate / potentialRate
            if (usedFraction < lowWatermark) {
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
                .map { CacheHost(4, 100, clock, remoteStorage, scheduler, metricRecorder) })
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

    fun currentSubmitRate(): Double {
        val currentWorkload = metricRecorder.submittedTasks.sumOf {
            val maxPossibleWorkload = clock.millis() - it.submitTime
            if (it.duration > maxPossibleWorkload) {
                maxPossibleWorkload
            } else {
                it.duration
            }
        }

        return currentWorkload.toDouble() / period.inWholeMilliseconds
    }

    fun currentServiceRate(): Double {
        val currentTime = clock.millis()
        val epochStart = currentTime - period.inWholeMilliseconds
        val serviceProvided = metricRecorder.completedTasks.sumOf {
            it.endTime - maxOf(it.startTime, epochStart)
        } +
            metricRecorder.inProcess.values.sumOf {
                currentTime - maxOf(it.startTime, epochStart)
            }

//        println(metricRecorder.inProcess.size)

        return serviceProvided.toDouble() / period.inWholeMilliseconds
    }
}
