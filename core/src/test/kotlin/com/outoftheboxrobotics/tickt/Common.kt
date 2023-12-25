package com.outoftheboxrobotics.tickt

import arrow.core.toNonEmptyListOrNull
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout

// Key set for testing
enum class TestKeys : TicketKey {
    A,
    B,
    C,
    D
}

val allTestKeys = TestKeys.entries.toNonEmptyListOrNull()!!

// Helper function to call runBlocking with a timeout
fun runTimeoutBlocking(timeout: Long = 100, block: suspend CoroutineScope.() -> Unit) = runBlocking {
    withTimeout(timeout, block)
}
