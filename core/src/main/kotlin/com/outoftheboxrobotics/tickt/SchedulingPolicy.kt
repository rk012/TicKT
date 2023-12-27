package com.outoftheboxrobotics.tickt

import arrow.core.Nel

interface SchedulingPolicy {
    sealed interface TicketAction {
        data object Drop : TicketAction
        data object Standby : TicketAction
        data class Queue(val dropExisting: Boolean) : TicketAction
    }

    fun schedule(ticket: Ticket, active: Set<Ticket>, queued: Set<Ticket>, standby: List<Ticket>): TicketAction

    fun select(standby: Nel<Ticket>): Ticket
}

object DefaultSchedulingPolicy : SchedulingPolicy {
    override fun schedule(
        ticket: Ticket,
        active: Set<Ticket>,
        queued: Set<Ticket>,
        standby: List<Ticket>
    ) = SchedulingPolicy.TicketAction.Queue(true)

    override fun select(standby: Nel<Ticket>) = standby.head
}

class DelegatedSchedulingPolicy(private val parent: TicketScheduler) : SchedulingPolicy {
    override fun schedule(
        ticket: Ticket,
        active: Set<Ticket>,
        queued: Set<Ticket>,
        standby: List<Ticket>
    ) = parent.schedulingPolicy.schedule(ticket, active, queued, standby)

    override fun select(standby: Nel<Ticket>) = parent.schedulingPolicy.select(standby)
}
