package org.opendc.storage.cache

import java.time.InstantSource
import kotlin.math.ceil
import kotlin.math.floor
import kotlin.time.Duration

class Autoscaler(val clock: InstantSource,
                 val remoteStorage: RemoteStorage,
                 val scheduler: TaskScheduler,
                 val metricRecorder: MetricRecorder,
    val watermarks: Pair<Double, Double>
) {


    val lowWatermark = watermarks.first
    val highWatermark = watermarks.second

    suspend fun autoscale() {
        val queuedCap = currentQueuedCap()
        val serviceCap = currentServiceCap()
        val availableCap = potentialServiceCap()

//        println("$serviceRate $submitRate")

        val requiredCap = serviceCap + queuedCap
        val incomingRatio = requiredCap / availableCap
//        println("${incomingRatio} ${submitRate} ${serviceRate}")
        var newPotentialRateDelta = 0.0
        // Is more than 90% of capacity being used
        if (incomingRatio > highWatermark) {
            // scale up
            // With an incoming ratio of 0.9, this gives us a new minimum service rate of 1.1*serviceRate
            newPotentialRateDelta = requiredCap - availableCap
        } else {
            // is less than 70% of capacity being used
            val usedFraction = requiredCap / availableCap
            if (usedFraction < lowWatermark) {
                // scale down
                newPotentialRateDelta = -(availableCap - requiredCap)
            }
        }
        val serverChange = ceil(newPotentialRateDelta/serviceRatePerServer).toInt()
//        println("$queuedCap $serviceCap $availableCap $serverChange")
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

    val serviceRatePerServer: Double = 4.0

    fun potentialServiceCap(): Double {
        return scheduler.hosts.size * serviceRatePerServer
    }

    fun currentQueuedCap(): Double {
        val currentWorkload = metricRecorder.queuedTasks.values.sumOf {
            val maxPossibleWorkload = clock.millis() - it.submitTime
            if (it.duration > maxPossibleWorkload) {
                maxPossibleWorkload
            } else {
                it.duration
            }
        }

        return currentWorkload.toDouble() / metricRecorder.period.inWholeMilliseconds
    }

    fun currentServiceCap(): Double {
        val currentTime = clock.millis()
        val epochStart = currentTime - metricRecorder.period.inWholeMilliseconds
        val serviceProvided = metricRecorder.completedTasks.sumOf {
            it.endTime - maxOf(it.startTime, epochStart)
        } +
            metricRecorder.inProcess.values.sumOf {
                currentTime - maxOf(it.startTime, epochStart)
            }

//        println(metricRecorder.inProcess.size)

        return serviceProvided.toDouble() / metricRecorder.period.inWholeMilliseconds
    }
}
