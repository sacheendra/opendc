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

package org.opendc.simulator.resources

import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import kotlinx.coroutines.*
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.opendc.simulator.core.runBlockingSimulation
import org.opendc.simulator.resources.consumer.SimSpeedConsumerAdapter
import org.opendc.simulator.resources.consumer.SimWorkConsumer
import org.opendc.utils.TimerScheduler

/**
 * A test suite for the [SimResourceSource] class.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class SimResourceSourceTest {
    @Test
    fun testSpeed() = runBlockingSimulation {
        val scheduler = TimerScheduler<Any>(coroutineContext, clock)
        val capacity = 4200.0
        val provider = SimResourceSource(capacity, clock, scheduler)

        val consumer = mockk<SimResourceConsumer>(relaxUnitFun = true)
        every { consumer.onNext(any()) }
            .returns(SimResourceCommand.Consume(1000 * capacity, capacity))
            .andThen(SimResourceCommand.Exit)

        try {
            val res = mutableListOf<Double>()
            val adapter = SimSpeedConsumerAdapter(consumer, res::add)

            provider.consume(adapter)

            assertEquals(listOf(0.0, capacity, 0.0), res) { "Speed is reported correctly" }
        } finally {
            scheduler.close()
            provider.close()
        }
    }

    @Test
    fun testAdjustCapacity() = runBlockingSimulation {
        val scheduler = TimerScheduler<Any>(coroutineContext, clock)
        val provider = SimResourceSource(1.0, clock, scheduler)

        val consumer = spyk(SimWorkConsumer(2.0, 1.0))

        try {
            coroutineScope {
                launch { provider.consume(consumer) }
                delay(1000)
                provider.capacity = 0.5
            }
            assertEquals(3000, clock.millis())
            verify(exactly = 1) { consumer.onCapacityChanged(any(), true) }
        } finally {
            scheduler.close()
            provider.close()
        }
    }

    @Test
    fun testSpeedLimit() = runBlockingSimulation {
        val scheduler = TimerScheduler<Any>(coroutineContext, clock)
        val capacity = 4200.0
        val provider = SimResourceSource(capacity, clock, scheduler)

        val consumer = mockk<SimResourceConsumer>(relaxUnitFun = true)
        every { consumer.onNext(any()) }
            .returns(SimResourceCommand.Consume(1000 * capacity, 2 * capacity))
            .andThen(SimResourceCommand.Exit)

        try {
            val res = mutableListOf<Double>()
            val adapter = SimSpeedConsumerAdapter(consumer, res::add)

            provider.consume(adapter)

            assertEquals(listOf(0.0, capacity, 0.0), res) { "Speed is reported correctly" }
        } finally {
            scheduler.close()
            provider.close()
        }
    }

    /**
     * Test to see whether no infinite recursion occurs when interrupting during [SimResourceConsumer.onStart] or
     * [SimResourceConsumer.onNext].
     */
    @Test
    fun testIntermediateInterrupt() = runBlockingSimulation {
        val scheduler = TimerScheduler<Any>(coroutineContext, clock)
        val capacity = 4200.0
        val provider = SimResourceSource(capacity, clock, scheduler)

        val consumer = object : SimResourceConsumer {
            override fun onStart(ctx: SimResourceContext) {
                ctx.interrupt()
            }

            override fun onNext(ctx: SimResourceContext): SimResourceCommand {
                return SimResourceCommand.Exit
            }
        }

        try {
            provider.consume(consumer)
        } finally {
            scheduler.close()
            provider.close()
        }
    }

    @Test
    fun testInterrupt() = runBlockingSimulation {
        val scheduler = TimerScheduler<Any>(coroutineContext, clock)
        val capacity = 4200.0
        val provider = SimResourceSource(capacity, clock, scheduler)
        lateinit var resCtx: SimResourceContext

        val consumer = object : SimResourceConsumer {
            var isFirst = true
            override fun onStart(ctx: SimResourceContext) {
                resCtx = ctx
            }

            override fun onNext(ctx: SimResourceContext): SimResourceCommand {
                assertEquals(0.0, ctx.remainingWork)
                return if (isFirst) {
                    isFirst = false
                    SimResourceCommand.Consume(4.0, 1.0)
                } else {
                    SimResourceCommand.Exit
                }
            }
        }

        try {
            launch {
                yield()
                resCtx.interrupt()
            }
            provider.consume(consumer)

            assertEquals(0, clock.millis())
        } finally {
            scheduler.close()
            provider.close()
        }
    }

    @Test
    fun testFailure() = runBlockingSimulation {
        val scheduler = TimerScheduler<Any>(coroutineContext, clock)
        val capacity = 4200.0
        val provider = SimResourceSource(capacity, clock, scheduler)

        val consumer = mockk<SimResourceConsumer>(relaxUnitFun = true)
        every { consumer.onStart(any()) }
            .throws(IllegalStateException())

        try {
            assertThrows<IllegalStateException> {
                provider.consume(consumer)
            }
        } finally {
            scheduler.close()
            provider.close()
        }
    }

    @Test
    fun testExceptionPropagationOnNext() = runBlockingSimulation {
        val scheduler = TimerScheduler<Any>(coroutineContext, clock)
        val capacity = 4200.0
        val provider = SimResourceSource(capacity, clock, scheduler)

        val consumer = mockk<SimResourceConsumer>(relaxUnitFun = true)
        every { consumer.onNext(any()) }
            .returns(SimResourceCommand.Consume(1.0, 1.0))
            .andThenThrows(IllegalStateException())

        try {
            assertThrows<IllegalStateException> {
                provider.consume(consumer)
            }
        } finally {
            scheduler.close()
            provider.close()
        }
    }

    @Test
    fun testConcurrentConsumption() = runBlockingSimulation {
        val scheduler = TimerScheduler<Any>(coroutineContext, clock)
        val capacity = 4200.0
        val provider = SimResourceSource(capacity, clock, scheduler)

        val consumer = mockk<SimResourceConsumer>(relaxUnitFun = true)
        every { consumer.onNext(any()) }
            .returns(SimResourceCommand.Consume(1.0, 1.0))
            .andThenThrows(IllegalStateException())

        try {
            assertThrows<IllegalStateException> {
                coroutineScope {
                    launch { provider.consume(consumer) }
                    provider.consume(consumer)
                }
            }
        } finally {
            scheduler.close()
            provider.close()
        }
    }

    @Test
    fun testClosedConsumption() = runBlockingSimulation {
        val scheduler = TimerScheduler<Any>(coroutineContext, clock)
        val capacity = 4200.0
        val provider = SimResourceSource(capacity, clock, scheduler)

        val consumer = mockk<SimResourceConsumer>(relaxUnitFun = true)
        every { consumer.onNext(any()) }
            .returns(SimResourceCommand.Consume(1.0, 1.0))
            .andThenThrows(IllegalStateException())

        try {
            assertThrows<IllegalStateException> {
                provider.close()
                provider.consume(consumer)
            }
        } finally {
            scheduler.close()
            provider.close()
        }
    }

    @Test
    fun testCloseDuringConsumption() = runBlockingSimulation {
        val scheduler = TimerScheduler<Any>(coroutineContext, clock)
        val capacity = 4200.0
        val provider = SimResourceSource(capacity, clock, scheduler)

        val consumer = mockk<SimResourceConsumer>(relaxUnitFun = true)
        every { consumer.onNext(any()) }
            .returns(SimResourceCommand.Consume(1.0, 1.0))
            .andThenThrows(IllegalStateException())

        try {
            launch { provider.consume(consumer) }
            delay(500)
            provider.close()

            assertEquals(500, clock.millis())
        } finally {
            scheduler.close()
            provider.close()
        }
    }

    @Test
    fun testIdle() = runBlockingSimulation {
        val scheduler = TimerScheduler<Any>(coroutineContext, clock)
        val capacity = 4200.0
        val provider = SimResourceSource(capacity, clock, scheduler)

        val consumer = mockk<SimResourceConsumer>(relaxUnitFun = true)
        every { consumer.onNext(any()) }
            .returns(SimResourceCommand.Idle(clock.millis() + 500))
            .andThen(SimResourceCommand.Exit)

        try {
            provider.consume(consumer)

            assertEquals(500, clock.millis())
        } finally {
            scheduler.close()
            provider.close()
        }
    }

    @Test
    fun testInfiniteSleep() {
        assertThrows<IllegalStateException> {
            runBlockingSimulation {
                val scheduler = TimerScheduler<Any>(coroutineContext, clock)
                val capacity = 4200.0
                val provider = SimResourceSource(capacity, clock, scheduler)

                val consumer = mockk<SimResourceConsumer>(relaxUnitFun = true)
                every { consumer.onNext(any()) }
                    .returns(SimResourceCommand.Idle())
                    .andThenThrows(IllegalStateException())

                try {
                    provider.consume(consumer)
                } finally {
                    scheduler.close()
                    provider.close()
                }
            }
        }
    }

    @Test
    fun testIncorrectDeadline() = runBlockingSimulation {
        val scheduler = TimerScheduler<Any>(coroutineContext, clock)
        val capacity = 4200.0
        val provider = SimResourceSource(capacity, clock, scheduler)

        val consumer = mockk<SimResourceConsumer>(relaxUnitFun = true)
        every { consumer.onNext(any()) }
            .returns(SimResourceCommand.Idle(2))
            .andThen(SimResourceCommand.Exit)

        try {
            delay(10)

            assertThrows<IllegalArgumentException> { provider.consume(consumer) }
        } finally {
            scheduler.close()
            provider.close()
        }
    }
}
