package com.outoftheboxrobotics.tickt

import arrow.core.Nel
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.first
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

interface TicketKey

data class Ticket(
    val requirements: Nel<TicketKey>,
    val action: suspend () -> Unit
)

internal class TicketScheduler(
    private val availableKeys: Nel<TicketKey>
): AbstractCoroutineContextElement(TicketScheduler) {
    companion object Key : CoroutineContext.Key<TicketScheduler>

    private sealed interface SchedulerStatus {
        data class QueueTicket(val ticket: Ticket) : SchedulerStatus
        data class TicketFinish(val ticket: Ticket, val cancelled: Boolean) : SchedulerStatus
    }

    // Channel is used here because the scheduler is the only observer
    private val statusChannel = Channel<SchedulerStatus>()

    // Flow is used here because multiple coroutines observe this to wait for task completion
    private val ticketFinishFlow = MutableSharedFlow<SchedulerStatus.TicketFinish>()

    private val queued = mutableSetOf<Ticket>()
    private val active = mutableMapOf<Ticket, Job>()

    internal suspend fun runScheduler() = coroutineScope {
        val iter = statusChannel.iterator()

        while (iter.hasNext()) {
            when(val status = iter.next()) {
                is SchedulerStatus.QueueTicket ->
                    if (status.ticket !in active) {
                        queued.removeIf {
                            val shouldRemove = (it.requirements intersect status.ticket.requirements).isNotEmpty()

                            if (shouldRemove)
                                launch { ticketFinishFlow.emit(SchedulerStatus.TicketFinish(it, true)) }

                            shouldRemove
                        }
                        queued.add(status.ticket)

                        active
                            .filterKeys { (it.requirements intersect status.ticket.requirements).isNotEmpty() }
                            .forEach { it.value.cancel() }
                    }

                    else continue

                is SchedulerStatus.TicketFinish -> active.remove(status.ticket)
            }

            queued.forEach { ticket ->
                if (active.keys.flatMap { it.requirements }.none { it in ticket.requirements }) {
                    val job = launch(start = CoroutineStart.LAZY) { ticket.action.invoke() }

                    active[ticket] = job

                    launch {
                        job.join()
                        val finish = SchedulerStatus.TicketFinish(ticket, job.isCancelled)
                        statusChannel.send(finish)
                        ticketFinishFlow.emit(finish)
                    }
                }
            }

            queued.removeAll(active.keys)
        }
    }

    internal fun cancel() = statusChannel.cancel()

    internal suspend fun runTicket(ticket: Ticket) = coroutineScope {
        require(availableKeys.containsAll(ticket.requirements)) { "Keys ${ticket.requirements} not all available in current context" }

        val isCancelled = async {
            ticketFinishFlow.first { it.ticket == ticket }.cancelled
        }

        statusChannel.send(SchedulerStatus.QueueTicket(ticket))

        if (isCancelled.await()) null else Unit
    }
}
