package com.outoftheboxrobotics.tickt

import arrow.core.Nel
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.first
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

/**
 * Unique identifier for a resource used by a [Ticket].
 */
interface TicketKey

/**
 * A task that requires a set of [TicketKeys][TicketKey] to run.
 *
 * @param requirements The set of [TicketKeys][TicketKey] required to run this task.
 * @param action The action to run when this task is started.
 */
class Ticket(
    val requirements: Nel<TicketKey>,
    val action: suspend () -> Unit
)

/**
 * A scheduler for running [Tickets][Ticket] that can be added to a coroutine context.
 *
 * Note that you usually don't need to create this yourself - use [ticketSchedulerContext] instead.
 */
class TicketScheduler(
    private val availableKeys: Nel<TicketKey>
): AbstractCoroutineContextElement(TicketScheduler) {
    companion object Key : CoroutineContext.Key<TicketScheduler>

    // Messages passed in the status channel
    private sealed interface SchedulerStatus {
        data class QueueTicket(val ticket: Ticket) : SchedulerStatus
        data class TicketFinish(val ticket: Ticket, val cancelled: Boolean) : SchedulerStatus
    }

    // Channel is used here because the scheduler is the only observer
    private val statusChannel = Channel<SchedulerStatus>()

    // Flow is used here because multiple coroutines observe this to wait for task completion
    private val ticketFinishFlow = MutableSharedFlow<SchedulerStatus.TicketFinish>()


    // Tasks waiting for other tasks to cancel
    private val queued = mutableSetOf<Ticket>()

    // Tasks currently running
    private val active = mutableMapOf<Ticket, Job>()

    /**
     * Runs the ticket scheduler indefinitely until [cancel] is called.
     */
    suspend fun runScheduler() = coroutineScope {
        val iter = statusChannel.iterator()

        while (iter.hasNext()) {
            when(val status = iter.next()) {
                // Add the ticket to the queue if it's not already running.
                // Remove tickets that have conflicting requirements.
                is SchedulerStatus.QueueTicket ->
                    if (status.ticket !in active) {
                        queued.removeIf {
                            val shouldRemove = (it.requirements intersect status.ticket.requirements).isNotEmpty()

                            if (shouldRemove)
                                launch { ticketFinishFlow.emit(SchedulerStatus.TicketFinish(it, true)) }

                            shouldRemove
                        }
                        queued.add(status.ticket)

                        // Cancel active tickets that have conflicting requirements
                        active
                            .filterKeys { (it.requirements intersect status.ticket.requirements).isNotEmpty() }
                            .forEach { it.value.cancel() }
                    }

                    else continue

                // Remove the ticket from the active set
                is SchedulerStatus.TicketFinish -> active.remove(status.ticket)
            }

            // Start any queued tickets if possible
            queued.forEach { ticket ->
                if (active.keys.flatMap { it.requirements }.none { it in ticket.requirements }) {
                    val job = launch(start = CoroutineStart.LAZY) { ticket.action.invoke() }

                    active[ticket] = job

                    // Start the job and emit the finish status once it's done
                    launch {
                        job.join()
                        val finish = SchedulerStatus.TicketFinish(ticket, job.isCancelled)
                        statusChannel.send(finish)
                        ticketFinishFlow.emit(finish)
                    }
                }
            }

            // Remove active tickets from the queue
            queued.removeAll(active.keys)
        }
    }

    /**
     * Cancels the scheduler.
     */
    fun cancel() = statusChannel.cancel()

    // Returns null if the ticket was cancelled, otherwise returns Unit
    internal suspend fun runTicket(ticket: Ticket) = coroutineScope {
        require(availableKeys.containsAll(ticket.requirements)) { "Keys ${ticket.requirements} not all available in current context" }

        // Monitor ticket's finish status
        val isCancelled = async {
            ticketFinishFlow.first { it.ticket == ticket }.cancelled
        }

        statusChannel.send(SchedulerStatus.QueueTicket(ticket))

        if (isCancelled.await()) null else Unit
    }
}
