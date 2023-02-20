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
            for (cb in callbacks) cb()
            resetMetrics()
            emit(Unit)
            delay(period)
        }
    }

    fun complete() {
        complete = true
    }

    fun addCallback(cb: suspend () -> Unit) {
        callbacks.add(cb)
    }

    val queuedTasks = HashMap<Long, CacheTask>()
    val inProcess = HashMap<Long, CacheTask>()
    var completedTasks = ArrayList<CacheTask>(10000)

    fun resetMetrics() {
        completedTasks = ArrayList(10000)

    }

    fun recordSubmission(task: CacheTask) {
        queuedTasks[task.taskId] = task
    }

    fun recordStart(task: CacheTask) {
        inProcess[task.taskId] = task
        queuedTasks.remove(task.taskId)
    }

    fun recordCompletion(task: CacheTask) {
        completedTasks.add(task)
        inProcess.remove(task.taskId)
    }
}
