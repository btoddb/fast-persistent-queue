package com.btoddb.fastpersitentqueue.chronicle;

import com.btoddb.fastpersitentqueue.chronicle.catchers.DirectCallCatcher;

import java.util.Collection;


/**
 *
 */
public class ErrorHandlerImpl extends ChronicleComponentBaseImpl implements ErrorHandler {
    private String catcher;

    @Override
    public void handle(Collection<FpqEvent> events) {
        RouteAndSnoop ras = config.getCatchers().get(catcher);
        ((DirectCallCatcher)ras.getCatcher()).inject(events);
    }

    @Override
    public void shutdown() {

    }

    public String getCatcher() {
        return catcher;
    }

    public void setCatcher(String catcher) {
        this.catcher = catcher;
    }
}
