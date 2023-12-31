package com.outoftheboxrobotics.tickt

import arrow.core.nel
import arrow.core.toNonEmptyListOrNull
import arrow.fx.coroutines.CyclicBarrier
import kotlinx.coroutines.*
import kotlin.test.*

class SchedulerTest {
    private suspend fun TicketScheduler.runTicket(vararg required: TicketKey, block: suspend () -> Unit) =
        runTicket(Ticket(required.asList().toNonEmptyListOrNull()!!, action = block))

    // Runs two tickets that use different keys
    @Test
    fun noConflictTest(): Unit = runTimeoutBlocking {
        val scheduler = TicketScheduler(allTestKeys)

        // Flags to check if the tickets ran
        var ab = false
        var cd = false

        launch { scheduler.runScheduler() }

        coroutineScope {
            launch {
                val res = scheduler.runTicket(TestKeys.A, TestKeys.B) {
                    ab = true
                }

                // Should finish successfully
                assertNotNull(res, "Interrupted ticket AB")
            }

            val res = scheduler.runTicket(TestKeys.C, TestKeys.D) {
                cd = true
            }

            // Should finish successfully
            assertNotNull(res, "Interrupted ticket CD")
        }

        assertTrue(ab, "Error in AB")
        assertTrue(cd, "Error in CD")

        scheduler.cancel()
    }

    // Runs two tickets that overlap in requirements
    @Test
    fun singleOverlapConflictTest(): Unit = runTimeoutBlocking {
        val scheduler = TicketScheduler(allTestKeys)

        var flag = false

        launch { scheduler.runScheduler() }

        coroutineScope {
            // Used to make sure the first ticket starts before the second
            val barrier = CyclicBarrier(2)

            launch {
                val res = scheduler.runTicket(TestKeys.A, TestKeys.B) {
                    barrier.await()

                    delay(Long.MAX_VALUE)
                }

                // Should be cancelled
                assertNull(res, "AB not cancelled")
            }

            barrier.await()

            val res = scheduler.runTicket(TestKeys.B, TestKeys.C) {
                flag = true
            }

            assertNotNull(res, "BC cancelled")
        }

        assertTrue(flag, "Flag not set")

        scheduler.cancel()
    }

    // Schedules the same ticket twice. All should succeed, but action should only run once.
    @Test
    fun duplicateTicketTest() = runTimeoutBlocking {
        val scheduler = TicketScheduler(allTestKeys)

        val barrier = CyclicBarrier(2)
        var counter = 0

        val ticket = Ticket(TestKeys.A.nel()) {
            counter++
            barrier.await()
        }

        launch { scheduler.runScheduler() }

        val results = List(10) {
            async { scheduler.runTicket(ticket) != null }
        }

        barrier.await()

        val allSuccess = results.awaitAll().all { it }

        assertEquals(1, counter, "Counter incremented multiple times")
        assertTrue(allSuccess, "Not all observers returned success")

        scheduler.cancel()
    }

    // Schedules conflicting tickets while one is in queue. The queued ticket should be replaced.
    @Test
    fun queueOverlapTest() = runTimeoutBlocking {
        val scheduler = TicketScheduler(allTestKeys)

        var counter = 0

        launch { scheduler.runScheduler() }

        coroutineScope {
            val startTaskBarrier = CyclicBarrier(2)
            val stepBarrier = CyclicBarrier(2)

            launch {
                scheduler.runTicket(TestKeys.A) {
                    // Forces other tickets to stay in queue until startTaskBarrier is reached
                    withContext(NonCancellable) {
                        stepBarrier.await()
                        startTaskBarrier.await()
                    }
                }
            }

            stepBarrier.await()

            launch {
                val res = scheduler.runTicket(TestKeys.A) {
                    counter++
                }

                assertNull(res, "Second task not cancelled")
            }

            yield()

            launch {
                val res = scheduler.runTicket(TestKeys.A) {
                    counter++
                }

                assertNotNull(res, "Final task cancelled")
            }

            yield()

            startTaskBarrier.await()
        }

        assertEquals(1, counter, "Counter incremented multiple times")

        scheduler.cancel()
    }
}