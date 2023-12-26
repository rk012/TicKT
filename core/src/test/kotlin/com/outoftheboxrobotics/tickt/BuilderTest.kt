package com.outoftheboxrobotics.tickt

import arrow.fx.coroutines.CyclicBarrier
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.assertThrows
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class BuilderTest {
    @Test
    fun simpleTicketTest() = runTimeoutBlocking {
        // Flags to set
        var ab = false
        var cd = false

        coroutineScope {
            launch {
                ab = withTicket(TestKeys.A, TestKeys.B) {
                    true
                }
            }

            launch {
                cd = withTicket(TestKeys.C, TestKeys.D) {
                    true
                }
            }
        }

        assertTrue(ab, "Ticket AB did not run")
        assertTrue(cd, "Ticket CD did not run")
    }

    @Test
    fun missingTicketContextTest() {
        // No scheduler in context
        val e = assertThrows<IllegalArgumentException> {
            runBlocking { withTicket(TestKeys.A) {} }
        }

        assertEquals("No ticket scheduler in coroutine context", e.message, "Unexpected exception")
    }

    @Test
    fun ticketCancelTest() = runTimeoutBlocking {
        coroutineScope {
            val barrier = CyclicBarrier(2)

            val job = launch {
                withTicket(TestKeys.A, TestKeys.B) {
                    barrier.await()
                    delay(Long.MAX_VALUE)
                }

                error("Job did not cancel")
            }

            barrier.await()

            // Should cancel the other ticket
            withTicket(TestKeys.B, TestKeys.C) {}

            assertTrue(job.isCompleted, "Job not cancelled")
            assertTrue(job.isCancelled, "Job did not cancel")
        }
    }
}