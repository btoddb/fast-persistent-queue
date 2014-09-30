package com.btoddb.fastpersitentqueue.eventbus;


/**
 * Created by burrb009 on 9/29/14.
 */
public interface FpqRouter extends EventBusComponent{
    /**
     * Return a {@link com.btoddb.fastpersitentqueue.eventbus.PlunkerRunner} if the event should be
     * routed to  it.
     *
     * @param catcherId ID of the {@link com.btoddb.fastpersitentqueue.eventbus.FpqCatcher} that caught this event
     * @param event The event to route
     * @return FpqRouter if route is satisfied, null otherwise
     */
    PlunkerRunner canRoute(String catcherId, FpqEvent event);
}
