package com.btoddb.fastpersitentqueue.chronicle.catchers;

import com.btoddb.fastpersitentqueue.chronicle.RouteAndSnoop;
import com.btoddb.fastpersitentqueue.chronicle.Config;
import com.btoddb.fastpersitentqueue.chronicle.FpqCatcher;
import com.btoddb.fastpersitentqueue.chronicle.FpqEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;


/**
 * Call this catcher directly from code to inject events into Chronicle
 */
public class DirectCallCatcherImpl extends CatcherBaseImpl implements FpqCatcher, DirectCallCatcher {
    private static final Logger logger = LoggerFactory.getLogger(DirectCallCatcherImpl.class);

    @Override
    public void init(Config config, RouteAndSnoop router) throws Exception {
        super.init(config, router);
    }

    @Override
    public void shutdown() {
        // nothing to do
    }

    @Override
    public void inject(Collection<FpqEvent> events) {
        catchEvents(events);
    }
}
