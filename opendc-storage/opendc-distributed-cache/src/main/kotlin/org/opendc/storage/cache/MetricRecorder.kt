package org.opendc.storage.cache

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach
import java.time.InstantSource
import kotlin.time.Duration

class MetricRecorder(
    val period: Duration
) {

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

    var submittedTasks = ArrayList<CacheTask>(10000)
    var inProcess = HashMap<Long, CacheTask>()
    var completedTasks = ArrayList<CacheTask>(10000)

    fun resetMetrics() {
        submittedTasks = ArrayList(10000)
        completedTasks = ArrayList(10000)

    }

    fun recordSubmission(task: CacheTask) {
        submittedTasks.add(task)
    }

    fun recordStart(task: CacheTask) {
        inProcess[task.taskId] = task
    }

    fun recordCompletion(task: CacheTask) {
        completedTasks.add(task)
        inProcess.remove(task.taskId)
    }
}
