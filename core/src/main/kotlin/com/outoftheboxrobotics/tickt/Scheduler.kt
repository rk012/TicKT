package com.outoftheboxrobotics.tickt

import arrow.core.Nel
import arrow.core.toNonEmptyListOrNull
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
    val priorityKey: Any? = null,
    val action: suspend () -> Unit
)

/**
 * A scheduler for running [Tickets][Ticket] that can be added to a coroutine context.
 *
 * Note that you usually don't need to create this yourself - use [ticketSchedulerContext] instead.
 */
class TicketScheduler(
    private val availableKeys: Nel<TicketKey>,
    var schedulingPolicy: SchedulingPolicy = DefaultSchedulingPolicy
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


    // Tasks waiting for an available slot. Ordered by recency.
    private val standby = mutableListOf<Ticket>()

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
                // Figure out what to do with the new ticket
                is SchedulerStatus.QueueTicket -> {
                    if (status.ticket in active) continue

                    when (
                        val action = schedulingPolicy.schedule(
                            status.ticket,
                            // Making defensive copies to prevent modification
                            active.keys.toSet(), queued.toSet(), standby.toList()
                        )
                    ) {
                        // Cancel the ticket
                        is SchedulingPolicy.TicketAction.Drop ->
                            launch { ticketFinishFlow.emit(SchedulerStatus.TicketFinish(status.ticket, true)) }

                        // Add the ticket to the standby list
                        is SchedulingPolicy.TicketAction.Standby -> standby.add(status.ticket)

                        // Add the ticket to the queue. If dropExisting is true, cancel any active tickets that have
                        // conflicting requirements. Otherwise, move them to the standby list.
                        is SchedulingPolicy.TicketAction.Queue -> {
                            queued.removeIf {
                                val shouldRemove = (it.requirements intersect status.ticket.requirements).isNotEmpty()

                                if (shouldRemove && action.dropExisting)
                                    launch { ticketFinishFlow.emit(SchedulerStatus.TicketFinish(it, true)) }
                                else standby.add(it)

                                shouldRemove
                            }
                            queued.add(status.ticket)

                            // Cancel active tickets that have conflicting requirements
                            active
                                .filterKeys { (it.requirements intersect status.ticket.requirements).isNotEmpty() }
                                .forEach { it.value.cancel() }
                        }
                    }
                }

                // Remove the ticket from the active set
                is SchedulerStatus.TicketFinish -> active.remove(status.ticket)
            }

            // Promote standby tickets to the queue if possible
            val availableTickets = standby.filter { ticket ->
                ((active.keys + queued).flatMap { it.requirements } intersect ticket.requirements).isEmpty()
            }.toMutableList()

            while (true) {
                // Break if there are no more tickets to promote (toNonEmptyListOrNull returns null if empty)
                val standbyNel = availableTickets.toNonEmptyListOrNull() ?: break

                val ticketToRemove = schedulingPolicy.select(standbyNel)

                // No need to cancel any conflicting active tickets - there shouldn't be any
                // Queued tickets will then be activated after this loop
                queued.add(ticketToRemove)

                availableTickets.removeIf { (it.requirements intersect ticketToRemove.requirements).isNotEmpty() }
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
