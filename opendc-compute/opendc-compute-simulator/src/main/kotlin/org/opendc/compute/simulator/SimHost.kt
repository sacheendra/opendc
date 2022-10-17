/*
 * Copyright (c) 2020 AtLarge Research
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

package org.opendc.compute.simulator

import kotlinx.coroutines.yield
import org.opendc.compute.api.Flavor
import org.opendc.compute.api.Server
import org.opendc.compute.api.ServerState
import org.opendc.compute.service.driver.Host
import org.opendc.compute.service.driver.HostListener
import org.opendc.compute.service.driver.HostModel
import org.opendc.compute.service.driver.HostState
import org.opendc.compute.service.driver.telemetry.GuestCpuStats
import org.opendc.compute.service.driver.telemetry.GuestSystemStats
import org.opendc.compute.service.driver.telemetry.HostCpuStats
import org.opendc.compute.service.driver.telemetry.HostSystemStats
import org.opendc.compute.simulator.internal.DefaultWorkloadMapper
import org.opendc.compute.simulator.internal.Guest
import org.opendc.compute.simulator.internal.GuestListener
import org.opendc.simulator.compute.SimBareMetalMachine
import org.opendc.simulator.compute.SimMachineContext
import org.opendc.simulator.compute.kernel.SimHypervisor
import org.opendc.simulator.compute.model.MachineModel
import org.opendc.simulator.compute.model.MemoryUnit
import org.opendc.simulator.compute.model.ProcessingNode
import org.opendc.simulator.compute.model.ProcessingUnit
import org.opendc.simulator.flow2.FlowGraph
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * A [Host] that is simulates virtual machines on a physical machine using [SimHypervisor].
 */
public class SimHost(
    override val uid: UUID,
    override val name: String,
    override val meta: Map<String, Any>,
    graph: FlowGraph,
    private val machine: SimBareMetalMachine,
    private val hypervisor: SimHypervisor,
    private val mapper: SimWorkloadMapper = DefaultWorkloadMapper,
    private val optimize: Boolean = false
) : Host, AutoCloseable {
    /**
     * The clock instance used by the host.
     */
    private val clock = graph.engine.clock

    /**
     * The event listeners registered with this host.
     */
    private val listeners = mutableListOf<HostListener>()

    /**
     * The virtual machines running on the hypervisor.
     */
    private val guests = HashMap<Server, Guest>()
    private val _guests = mutableListOf<Guest>()

    override val instances: Set<Server>
        get() = guests.keys

    override val state: HostState
        get() = _state
    private var _state: HostState = HostState.DOWN
        set(value) {
            if (value != field) {
                listeners.forEach { it.onStateChanged(this, value) }
            }
            field = value
        }

    override val model: HostModel = HostModel(
        machine.model.cpus.sumOf { it.frequency },
        machine.model.cpus.size,
        machine.model.memory.sumOf { it.size }
    )

    /**
     * The [GuestListener] that listens for guest events.
     */
    private val guestListener = object : GuestListener {
        override fun onStart(guest: Guest) {
            listeners.forEach { it.onStateChanged(this@SimHost, guest.server, guest.state) }
        }

        override fun onStop(guest: Guest) {
            listeners.forEach { it.onStateChanged(this@SimHost, guest.server, guest.state) }
        }
    }

    init {
        launch()
    }

    override fun canFit(server: Server): Boolean {
        val sufficientMemory = model.memoryCapacity >= server.flavor.memorySize
        val enoughCpus = model.cpuCount >= server.flavor.cpuCount
        val canFit = hypervisor.canFit(server.flavor.toMachineModel())

        return sufficientMemory && enoughCpus && canFit
    }

    override suspend fun spawn(server: Server, start: Boolean) {
        val guest = guests.computeIfAbsent(server) { key ->
            require(canFit(key)) { "Server does not fit" }

            val machine = hypervisor.newMachine(key.flavor.toMachineModel())
            val newGuest = Guest(
                clock,
                this,
                hypervisor,
                mapper,
                guestListener,
                server,
                machine
            )

            _guests.add(newGuest)
            newGuest
        }

        if (start) {
            guest.start()
        }
    }

    override fun contains(server: Server): Boolean {
        return server in guests
    }

    override suspend fun start(server: Server) {
        val guest = requireNotNull(guests[server]) { "Unknown server ${server.uid} at host $uid" }
        guest.start()
    }

    override suspend fun stop(server: Server) {
        val guest = requireNotNull(guests[server]) { "Unknown server ${server.uid} at host $uid" }
        guest.stop()
    }

    override suspend fun delete(server: Server) {
        val guest = guests[server] ?: return
        guest.delete()
    }

    override fun addListener(listener: HostListener) {
        listeners.add(listener)
    }

    override fun removeListener(listener: HostListener) {
        listeners.remove(listener)
    }

    override fun close() {
        reset(HostState.DOWN)
        machine.cancel()
    }

    override fun getSystemStats(): HostSystemStats {
        updateUptime()

        var terminated = 0
        var running = 0
        var error = 0
        var invalid = 0

        val guests = _guests.listIterator()
        for (guest in guests) {
            when (guest.state) {
                ServerState.TERMINATED -> terminated++
                ServerState.RUNNING -> running++
                ServerState.ERROR -> error++
                ServerState.DELETED -> {
                    // Remove guests that have been deleted
                    this.guests.remove(guest.server)
                    guests.remove()
                }
                else -> invalid++
            }
        }

        return HostSystemStats(
            Duration.ofMillis(_uptime),
            Duration.ofMillis(_downtime),
            _bootTime,
            machine.psu.powerUsage,
            machine.psu.energyUsage,
            terminated,
            running,
            error,
            invalid
        )
    }

    override fun getSystemStats(server: Server): GuestSystemStats {
        val guest = requireNotNull(guests[server]) { "Unknown server ${server.uid} at host $uid" }
        return guest.getSystemStats()
    }

    override fun getCpuStats(): HostCpuStats {
        val counters = hypervisor.counters
        counters.sync()

        return HostCpuStats(
            counters.cpuActiveTime / 1000L,
            counters.cpuIdleTime / 1000L,
            counters.cpuStealTime / 1000L,
            counters.cpuLostTime / 1000L,
            hypervisor.cpuCapacity,
            hypervisor.cpuDemand,
            hypervisor.cpuUsage,
            hypervisor.cpuUsage / _cpuLimit
        )
    }

    override fun getCpuStats(server: Server): GuestCpuStats {
        val guest = requireNotNull(guests[server]) { "Unknown server ${server.uid} at host $uid" }
        return guest.getCpuStats()
    }

    override fun hashCode(): Int = uid.hashCode()

    override fun equals(other: Any?): Boolean {
        return other is SimHost && uid == other.uid
    }

    override fun toString(): String = "SimHost[uid=$uid,name=$name,model=$model]"

    public fun fail() {
        reset(HostState.ERROR)

        for (guest in _guests) {
            guest.fail()
        }
    }

    public suspend fun recover() {
        updateUptime()

        launch()

        // Wait for the hypervisor to launch before recovering the guests
        yield()

        for (guest in _guests) {
            guest.recover()
        }
    }

    /**
     * The [SimMachineContext] that represents the machine running the hypervisor.
     */
    private var _ctx: SimMachineContext? = null

    /**
     * Launch the hypervisor.
     */
    private fun launch() {
        check(_ctx == null) { "Concurrent hypervisor running" }

        // Launch hypervisor onto machine
        _bootTime = clock.instant()
        _state = HostState.UP
        _ctx = machine.startWorkload(hypervisor, emptyMap()) { cause ->
            _state = if (cause != null) HostState.ERROR else HostState.DOWN
            _ctx = null
        }
    }

    /**
     * Reset the machine.
     */
    private fun reset(state: HostState) {
        updateUptime()

        // Stop the hypervisor
        _ctx?.shutdown()
        _state = state
    }

    /**
     * Convert flavor to machine model.
     */
    private fun Flavor.toMachineModel(): MachineModel {
        val originalCpu = machine.model.cpus[0]
        val originalNode = originalCpu.node
        val cpuCapacity = (this.meta["cpu-capacity"] as? Double ?: Double.MAX_VALUE).coerceAtMost(originalCpu.frequency)
        val processingNode = ProcessingNode(originalNode.vendor, originalNode.modelName, originalNode.architecture, cpuCount)
        val processingUnits = (0 until cpuCount).map { ProcessingUnit(processingNode, it, cpuCapacity) }
        val memoryUnits = listOf(MemoryUnit("Generic", "Generic", 3200.0, memorySize))

        val model = MachineModel(processingUnits, memoryUnits)
        return if (optimize) model.optimize() else model
    }

    private var _lastReport = clock.millis()
    private var _uptime = 0L
    private var _downtime = 0L
    private var _bootTime: Instant? = null
    private val _cpuLimit = machine.model.cpus.sumOf { it.frequency }

    /**
     * Helper function to track the uptime of a machine.
     */
    private fun updateUptime() {
        val now = clock.millis()
        val duration = now - _lastReport
        _lastReport = now

        if (_state == HostState.UP) {
            _uptime += duration
        } else if (_state == HostState.ERROR) {
            // Only increment downtime if the machine is in a failure state
            _downtime += duration
        }

        val guests = _guests
        for (i in guests.indices) {
            guests[i].updateUptime()
        }
    }
}
