package com.btoddb.fastpersitentqueue.eventbus.plunkers;

import com.btoddb.fastpersitentqueue.eventbus.FpqEvent;

import java.util.Collection;


/**
 *
 */
public class GarbagePlunkerImpl extends PlunkerBaseImpl {
    @Override
    public boolean handleInternal(Collection<FpqEvent> events) throws Exception {
        // nothing to do
        return true;
    }

    @Override
    public void shutdown() {
        // nothing to do
    }
}
