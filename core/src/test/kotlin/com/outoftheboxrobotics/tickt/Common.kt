package com.outoftheboxrobotics.tickt

import arrow.core.toNonEmptyListOrNull
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout

enum class TestKeys : TicketKey {
    A,
    B,
    C,
    D
}

val allTestKeys = TestKeys.entries.toNonEmptyListOrNull()!!

fun runTimeoutBlocking(timeout: Long = 100, block: suspend CoroutineScope.() -> Unit) = runBlocking {
    withTimeout(timeout, block)
}
