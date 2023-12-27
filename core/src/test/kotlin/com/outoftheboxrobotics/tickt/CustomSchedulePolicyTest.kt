package com.outoftheboxrobotics.tickt

import arrow.core.Nel
import arrow.fx.coroutines.CyclicBarrier
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CustomSchedulePolicyTest {
    @Test
    fun standbyTest() = runTimeoutBlocking(
        schedulingPolicy = object : SchedulingPolicy {
            override fun schedule(
                ticket: Ticket,
                active: Set<Ticket>,
                queued: Set<Ticket>,
                standby: List<Ticket>
            ) = SchedulingPolicy.TicketAction.Standby

            override fun select(standby: Nel<Ticket>) = standby.head
        }
    ) {
        var flag = false

        val b0 = CyclicBarrier(3)
        val b1 = CyclicBarrier(2)

        coroutineScope {
            val job = launch {
                withTicket(TestKeys.A, TestKeys.B) {
                    b0.await()
                    b1.await()
                }
            }

            launch {
                b0.await()
                withTicket(TestKeys.B, TestKeys.C) {
                    flag = true
                }
            }

            // Guarantees that first ticket is running and second has a chance to be scheduled
            b0.await()
            yield()

            assertFalse(flag, "Other job ran")
            assertFalse(job.isCancelled, "First job already cancelled")

            b1.await()
        }

        assertTrue(flag, "Flag not set")
    }

    @Test
    fun nestedSchedulerTest() {
        var counter = 0

        runTimeoutBlocking(
            schedulingPolicy = object : SchedulingPolicy by DefaultSchedulingPolicy {
                override fun schedule(
                    ticket: Ticket,
                    active: Set<Ticket>,
                    queued: Set<Ticket>,
                    standby: List<Ticket>
                ) = DefaultSchedulingPolicy.schedule(ticket, active, queued, standby).also { counter++ }
            }
        ) {
            withTicket(TestKeys.A, TestKeys.B) {
                withTicket(TestKeys.A) {
                    // Do nothing
                }
            }

            assertEquals(2, counter, "Incorrect number of scheduling policy calls")
        }
    }
}