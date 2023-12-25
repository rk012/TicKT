package com.outoftheboxrobotics.tickt

import arrow.core.Nel
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.filterIsInstance
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.takeWhile
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
        data object StopScheduler : SchedulerStatus
    }

    private val statusFlow = MutableSharedFlow<SchedulerStatus>()

    private val queued = mutableSetOf<Ticket>()
    private val active = mutableMapOf<Ticket, Job>()

    internal suspend fun runScheduler() = coroutineScope {
        statusFlow.takeWhile { it !is SchedulerStatus.StopScheduler }.collect { status ->
            when(status) {
                is SchedulerStatus.QueueTicket ->
                    if (status.ticket !in active) {
                        queued.removeIf { (it.requirements intersect status.ticket.requirements).isNotEmpty() }
                        queued.add(status.ticket)

                        active
                            .filterKeys { (it.requirements intersect status.ticket.requirements).isNotEmpty() }
                            .forEach { it.value.cancel() }
                    }

                    else return@collect

                is SchedulerStatus.TicketFinish -> active.remove(status.ticket)

                is SchedulerStatus.StopScheduler -> error("StopScheduler in runScheduler status collector")
            }

            queued.forEach { ticket ->
                if (active.keys.flatMap { it.requirements }.none { it in ticket.requirements }) {
                    val job = launch(start = CoroutineStart.LAZY) { ticket.action.invoke() }

                    active[ticket] = job

                    launch {
                        job.join()
                        statusFlow.emit(SchedulerStatus.TicketFinish(ticket, job.isCancelled))
                    }
                }
            }

            queued.removeAll(active.keys)
        }
    }

    internal suspend fun cancel() = statusFlow.emit(SchedulerStatus.StopScheduler)

    internal suspend fun runTicket(ticket: Ticket) = coroutineScope {
        require(ticket.requirements.containsAll(availableKeys)) { "Keys ${ticket.requirements} not all available in current context" }

        val isCancelled = async {
            statusFlow.filterIsInstance<SchedulerStatus.TicketFinish>().first { it.ticket == ticket }.cancelled
        }

        statusFlow.emit(SchedulerStatus.QueueTicket(ticket))

        if (isCancelled.await()) null else Unit
    }
}
