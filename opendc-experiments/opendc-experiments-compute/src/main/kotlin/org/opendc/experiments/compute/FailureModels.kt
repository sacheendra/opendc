/*
 * Copyright (c) 2021 AtLarge Research
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

@file:JvmName("FailureModels")

package org.opendc.experiments.compute

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.apache.commons.math3.distribution.LogNormalDistribution
import org.apache.commons.math3.distribution.RealDistribution
import org.apache.commons.math3.random.Well19937c
import org.opendc.compute.service.ComputeService
import org.opendc.compute.simulator.SimHost
import org.opendc.compute.simulator.failure.HostFault
import org.opendc.compute.simulator.failure.HostFaultInjector
import org.opendc.compute.simulator.failure.StartStopHostFault
import org.opendc.compute.simulator.failure.StochasticVictimSelector
import org.opendc.compute.simulator.failure.VictimSelector
import org.opendc.simulator.compute.workload.SimRuntimeWorkload
import org.opendc.trace.failure.FailureTraceReader
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Clock
import java.time.Duration
import java.util.Random
import kotlin.coroutines.CoroutineContext
import kotlin.math.ln
import kotlin.math.roundToInt
import kotlin.math.roundToLong

/**
 * Obtain a [FailureModel] based on the GRID'5000 failure trace.
 *
 * This fault injector uses parameters from the GRID'5000 failure trace as described in
 * "A Framework for the Study of Grid Inter-Operation Mechanisms", A. Iosup, 2009.
 */
public fun grid5000(failureInterval: Duration): FailureModel {
    return object : FailureModel {
        override fun createInjector(
            context: CoroutineContext,
            clock: Clock,
            service: ComputeService,
            random: Random
        ): HostFaultInjector {
            val rng = Well19937c(random.nextLong())
            val hosts = service.hosts.map { it as SimHost }.toSet()

            // Parameters from A. Iosup, A Framework for the Study of Grid Inter-Operation Mechanisms, 2009
            // GRID'5000
            return HostFaultInjector(
                context,
                clock,
                hosts,
                iat = LogNormalDistribution(rng, ln(failureInterval.toHours().toDouble()), 1.03),
                selector = StochasticVictimSelector(LogNormalDistribution(rng, 1.88, 1.25), random),
                fault = StartStopHostFault(LogNormalDistribution(rng, 8.89, 2.71))
            )
        }

        override fun toString(): String = "Grid5000FailureModel"
    }
}

public fun cloudUptimeArchive(traceName: String, failureInterval: Duration): FailureModel {
    return object : FailureModel {
        override fun createInjector(
            context: CoroutineContext,
            clock: Clock,
            service: ComputeService,
            random: Random
        ): HostFaultInjector {
            val hosts = service.hosts.map { it as SimHost }.toSet()

            return TraceBasedFaultInjector(
                context,
                clock,
                hosts,
                traceName
            )
        }

        override fun toString(): String = "CloudUptimeArchiveFailureModel"
    }
}

public class TraceBasedFaultInjector(
    private val context: CoroutineContext,
    private val clock: Clock,
    private val hosts: Set<SimHost>,
    private val traceName: String
) : HostFaultInjector {
    /**
     * The scope in which the injector runs.
     */
    private val scope = CoroutineScope(context + Job())

    /**
     * The [Job] that awaits the nearest fault in the system.
     */
    private var job: Job? = null

    private var failureList = FailureTraceReader.get(Paths.get("src/test/resources/failure_traces/test1.csv")).iterator()

    /**
     * Start the fault injection into the system.
     */
    override fun start() {
        if (job != null) {
            return
        }

        job = scope.launch {
            runInjector()
            job = null
        }
    }

    /**
     * Converge the injection process.
     */
    private suspend fun runInjector() {
        while (failureList.hasNext()) {
            val failure = failureList.next()

            delay(failure.start - clock.millis())

            val numVictims = (failure.intensity * hosts.size).roundToInt()
            val victims = hosts.shuffled().take(numVictims)

            scope.launch {
                for (host in victims) {
                    host.fail()
                }

                // Handle long overflow
                if (clock.millis() + failure.duration <= 0) {
                    return@launch
                }

                delay(failure.duration)

                for (host in victims) {
                    host.recover()
                }
            }
        }
    }

    /**
     * Stop the fault injector.
     */
    public override fun close() {
        scope.cancel()
    }
}

public interface DurationHostFault {
    /**
     * Apply the fault to the specified [victims].
     */
    public suspend fun apply(clock: Clock, duration: Long, victims: List<SimHost>)
}

public class StopHostFault : DurationHostFault {

    override suspend fun apply(clock: Clock, duration: Long, victims: List<SimHost>) {
        for (host in victims) {
            host.fail()
        }

        // Handle long overflow
        if (clock.millis() + duration <= 0) {
            return
        }

        delay(duration)

        for (host in victims) {
            host.recover()
        }
    }

    override fun toString(): String = "StopHostFault"
}

public class CheckpointHostFault : DurationHostFault {

    override suspend fun apply(clock: Clock, duration: Long, victims: List<SimHost>) {
        for (host in victims) {
            host.fail()
            val servers = host.instances.toList()
            for (server in servers) {
                host.delete(server)
                val workload = server.meta["workload"] as SimRuntimeWorkload
            }
        }

        // Handle long overflow
        if (clock.millis() + duration <= 0) {
            return
        }

        delay(duration)

        for (host in victims) {
            host.recover()
        }
    }

    override fun toString(): String = "StopHostFault"
}
