package com.outoftheboxrobotics.tickt

import arrow.core.nonEmptyListOf
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class SchedulerTest {
    @Test
    fun noConflictTest(): Unit = runTimeoutBlocking {
        val scheduler = TicketScheduler(allTestKeys)

        var ab = false
        var cd = false

        launch {
            scheduler.runScheduler()
        }

        coroutineScope {
            launch {
                val res = scheduler.runTicket(Ticket(nonEmptyListOf(TestKeys.A, TestKeys.B)) {
                    ab = true
                })

                assertNotNull(res, "Interrupted ticket AB")
            }

            val res = scheduler.runTicket(Ticket(nonEmptyListOf(TestKeys.C, TestKeys.D)) {
                cd = true
            })

            assertNotNull(res, "Interrupted ticket CD")
        }

        assertTrue(ab, "Error in AB")
        assertTrue(cd, "Error in CD")

        scheduler.cancel()
    }
}