package org.opendc.storage.cache

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach
import kotlin.time.Duration

class MetricRecorder(val period: Duration) {

    val callbacks: MutableCollection<suspend () -> Unit> = mutableListOf()

    var complete = false
    val metricsFlow: Flow<Unit> = flow<Unit> {
        delay(period)
        while (!complete) {
            emit(Unit)
            delay(period)
        }
    }
        .onEach {
            for (cb in callbacks) cb()
            resetMetrics()
        }

    fun complete() {
        complete = true
    }

    fun addCallback(cb: suspend () -> Unit) {
        callbacks.add(cb)
    }

    var submittedTaskDurations = ArrayList<Long>(10000)
    var completedTaskDurations = ArrayList<Long>(10000)

    fun resetMetrics() {
        submittedTaskDurations = ArrayList<Long>(10000)
        completedTaskDurations = ArrayList<Long>(10000)
    }

    fun recordSubmission(task: CacheTask) {
        submittedTaskDurations.add(task.duration)
    }

    fun recordCompletion(task: CacheTask) {
        completedTaskDurations.add(task.duration)
    }
}
