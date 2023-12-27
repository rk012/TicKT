package com.outoftheboxrobotics.tickt

import arrow.core.*
import kotlinx.coroutines.*
import kotlin.coroutines.coroutineContext

/**
 * Creates a new coroutine context with a [TicketScheduler] that has the given [available] keys.
 *
 * @param available The keys that are available to the scheduler.
 * @param block The block to run in the new context.
 *
 * @return The result of [block].
 */
suspend fun <T> ticketSchedulerContext(
    available: Nel<TicketKey>,
    schedulingPolicy: SchedulingPolicy = DefaultSchedulingPolicy,
    block: suspend CoroutineScope.() -> T
) =
    coroutineScope {
        val scheduler = TicketScheduler(available, schedulingPolicy)

        launch { scheduler.runScheduler() }

        withContext(scheduler, block).also { scheduler.cancel() }
    }

/**
 * Runs a [Ticket] using the ticket scheduler in the current coroutine context.
 */
suspend fun runTicket(ticket: Ticket) {
    val scheduler = requireNotNull(coroutineContext[TicketScheduler]) { "No ticket scheduler in coroutine context" }

    scheduler.runTicket(ticket) ?: throw CancellationException("Ticket cancelled by scheduler")
}

/**
 * Creates and runs a new [Ticket] using the ticket scheduler in the current coroutine context.
 *
 * @param required The keys required to run the coroutine.
 * @param block The coroutine to run.
 *
 * @return The result of [block].
 */
suspend fun <T> withTicket(required: Nel<TicketKey>, block: suspend CoroutineScope.() -> T): T {
    val scheduler = requireNotNull(coroutineContext[TicketScheduler]) { "No ticket scheduler in coroutine context" }

    var res: Option<T> = none()

    runTicket(Ticket(required) {
        res = ticketSchedulerContext(required, DelegatedSchedulingPolicy(scheduler), block).some()
    })

    return res.getOrElse { error("Uninitialized result with successful ticket") }
}

/**
 * Convenience function for [withTicket] that takes a vararg of [TicketKey]s.
 *
 * @param required The keys required to run the coroutine.
 * @param block The coroutine to run.
 *
 * @return The result of [block].
 *
 * @see withTicket
 */
suspend fun <T> withTicket(vararg required: TicketKey, block: suspend CoroutineScope.() -> T) =
    withTicket(required.asList().toNonEmptyListOrNull() ?: error("No requirements specified"), block)
