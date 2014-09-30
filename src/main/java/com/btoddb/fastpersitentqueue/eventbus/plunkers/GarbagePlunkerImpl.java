package com.btoddb.fastpersitentqueue.eventbus.plunkers;

import com.btoddb.fastpersitentqueue.eventbus.EventBusComponentBaseImpl;
import com.btoddb.fastpersitentqueue.eventbus.FpqEvent;
import com.btoddb.fastpersitentqueue.eventbus.FpqPlunker;

import java.util.Collection;


/**
 *
 */
public class GarbagePlunkerImpl extends EventBusComponentBaseImpl implements FpqPlunker {
    @Override
    public boolean handle(Collection<FpqEvent> events) throws Exception {
        // nothing to do
        return true;
    }

    @Override
    public void shutdown() {
        // nothing to do
    }
}
