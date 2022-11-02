package org.opendc.experiments.capelin

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.opendc.compute.service.ComputeService
import org.opendc.compute.service.scheduler.FilterScheduler
import org.opendc.compute.service.scheduler.filters.ComputeFilter
import org.opendc.compute.service.scheduler.filters.RamFilter
import org.opendc.compute.service.scheduler.filters.VCpuFilter
import org.opendc.compute.service.scheduler.weights.CoreRamWeigher
import org.opendc.experiments.capelin.topology.clusterTopology
import org.opendc.experiments.compute.ComputeWorkloadLoader
import org.opendc.experiments.compute.VirtualMachine
import org.opendc.experiments.compute.cloudUptimeArchive
import org.opendc.experiments.compute.grid5000
import org.opendc.experiments.compute.registerComputeMonitor
import org.opendc.experiments.compute.replay
import org.opendc.experiments.compute.sampleByLoad
import org.opendc.experiments.compute.setupComputeService
import org.opendc.experiments.compute.setupHosts
import org.opendc.experiments.compute.telemetry.ComputeMonitor
import org.opendc.experiments.compute.telemetry.table.HostTableReader
import org.opendc.experiments.compute.telemetry.table.ServiceTableReader
import org.opendc.experiments.compute.topology.HostSpec
import org.opendc.experiments.compute.trace
import org.opendc.experiments.provisioner.Provisioner
import org.opendc.simulator.kotlin.runSimulation
import java.io.File
import java.time.Duration
import java.util.Random

class CloudUptimeArchive {
    /**
     * The monitor used to keep track of the metrics.
     */
    private lateinit var monitor: TestComputeMonitor

    /**
     * The [FilterScheduler] to use for all experiments.
     */
    private lateinit var computeScheduler: FilterScheduler

    /**
     * The [ComputeWorkloadLoader] responsible for loading the traces.
     */
    private lateinit var workloadLoader: ComputeWorkloadLoader

    /**
     * Set up the experimental environment.
     */
    @BeforeEach
    fun setUp() {
        monitor = TestComputeMonitor()
        computeScheduler = FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(16.0), RamFilter(1.0)),
            weighers = listOf(CoreRamWeigher(multiplier = 1.0))
        )
        workloadLoader = ComputeWorkloadLoader(File("src/test/resources/trace"))
    }

    @Test
    fun testFailures() = runSimulation {
        val seed = 0L
        val topology = createTopology("single")
        val workload = createTestWorkload(0.25, seed)
        val monitor = monitor

        Provisioner(coroutineContext, clock, seed).use { provisioner ->
            provisioner.runSteps(
                setupComputeService(serviceDomain = "compute.opendc.org", { computeScheduler }),
                registerComputeMonitor(serviceDomain = "compute.opendc.org", monitor),
                setupHosts(serviceDomain = "compute.opendc.org", topology)
            )

            val service = provisioner.registry.resolve("compute.opendc.org", ComputeService::class.java)!!
            service.replay(clock, workload, seed, failureModel = cloudUptimeArchive("instagram", Duration.ofDays(7)))
        }

        // Note that these values have been verified beforehand
        assertAll(
            { Assertions.assertEquals(10085103, monitor.idleTime) { "Idle time incorrect" } },
            { Assertions.assertEquals(8539212, monitor.activeTime) { "Active time incorrect" } },
            { Assertions.assertEquals(0, monitor.stealTime) { "Steal time incorrect" } },
            { Assertions.assertEquals(0, monitor.lostTime) { "Lost time incorrect" } },
            { Assertions.assertEquals(2328039558, monitor.uptime) { "Uptime incorrect" } }
        )
    }

    /**
     * Obtain the trace reader for the test.
     */
    private fun createTestWorkload(fraction: Double, seed: Long): List<VirtualMachine> {
        val source = trace("bitbrains-small").sampleByLoad(fraction)
        return source.resolve(workloadLoader, Random(seed))
    }

    /**
     * Obtain the topology factory for the test.
     */
    private fun createTopology(name: String = "topology"): List<HostSpec> {
        val stream = checkNotNull(object {}.javaClass.getResourceAsStream("/env/$name.txt"))
        return stream.use { clusterTopology(stream) }
    }

    class TestComputeMonitor : ComputeMonitor {
        var attemptsSuccess = 0
        var attemptsFailure = 0
        var attemptsError = 0
        var serversPending = 0
        var serversActive = 0

        override fun record(reader: ServiceTableReader) {
            attemptsSuccess = reader.attemptsSuccess
            attemptsFailure = reader.attemptsFailure
            attemptsError = reader.attemptsError
            serversPending = reader.serversPending
            serversActive = reader.serversActive
        }

        var idleTime = 0L
        var activeTime = 0L
        var stealTime = 0L
        var lostTime = 0L
        var energyUsage = 0.0
        var uptime = 0L

        override fun record(reader: HostTableReader) {
            idleTime += reader.cpuIdleTime
            activeTime += reader.cpuActiveTime
            stealTime += reader.cpuStealTime
            lostTime += reader.cpuLostTime
            energyUsage += reader.powerTotal
            uptime += reader.uptime
        }
    }
}
