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

package org.opendc.simulator.compute

import org.opendc.simulator.compute.model.ProcessingUnit
import org.opendc.simulator.compute.power.PowerDriver
import org.opendc.simulator.resources.*
import org.opendc.simulator.resources.SimResourceInterpreter

/**
 * A simulated bare-metal machine that is able to run a single workload.
 *
 * A [SimBareMetalMachine] is a stateful object and you should be careful when operating this object concurrently. For
 * example, the class expects only a single concurrent call to [run].
 *
 * @param interpreter The [SimResourceInterpreter] to drive the simulation.
 * @param model The machine model to simulate.
 * @param powerDriver The power driver to use.
 * @param psu The power supply of the machine.
 * @param parent The parent simulation system.
 */
public class SimBareMetalMachine(
    interpreter: SimResourceInterpreter,
    model: SimMachineModel,
    powerDriver: PowerDriver,
    public val psu: SimPsu = SimPsu(500.0, mapOf(1.0 to 1.0)),
    parent: SimResourceSystem? = null,
) : SimAbstractMachine(interpreter, parent, model) {
    /**
     * The processing units of the machine.
     */
    override val cpus: List<SimProcessingUnit> = model.cpus.map { cpu ->
        Cpu(SimResourceSource(cpu.frequency, interpreter, this@SimBareMetalMachine), cpu)
    }

    override fun updateUsage(usage: Double) {
        super.updateUsage(usage)
        psu.update()
    }

    init {
        psu.connect(powerDriver.createLogic(this, cpus))
    }

    /**
     * A [SimProcessingUnit] of a bare-metal machine.
     */
    private class Cpu(
        private val source: SimResourceSource,
        override val model: ProcessingUnit
    ) : SimProcessingUnit, SimResourceProvider by source {
        override var capacity: Double
            get() = source.capacity
            set(value) {
                // Clamp the capacity of the CPU between [0.0, maxFreq]
                source.capacity = value.coerceIn(0.0, model.frequency)
            }

        override fun toString(): String = "SimBareMetalMachine.Cpu[model=$model]"
    }
}
