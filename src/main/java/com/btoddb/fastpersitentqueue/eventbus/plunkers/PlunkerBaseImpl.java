package com.btoddb.fastpersitentqueue.eventbus.plunkers;

import com.btoddb.fastpersitentqueue.eventbus.BusMetrics;
import com.btoddb.fastpersitentqueue.eventbus.EventBusComponentBaseImpl;
import com.btoddb.fastpersitentqueue.eventbus.FpqEvent;
import com.btoddb.fastpersitentqueue.eventbus.FpqPlunker;

import java.util.Collection;


/**
 *
 */
public abstract class PlunkerBaseImpl extends EventBusComponentBaseImpl implements FpqPlunker {
    protected abstract boolean handleInternal(Collection<FpqEvent> events) throws Exception;


    @Override
    public final boolean handle(Collection<FpqEvent> events) throws Exception {
        try {
            BusMetrics.registry.meter(getId()).mark(events.size());
            return handleInternal(events);
        }
        catch (Exception e) {

        }
        finally {

        }

        return false;
    }
}
