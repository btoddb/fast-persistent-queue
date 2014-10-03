package com.btoddb.fastpersitentqueue.chronicle.catchers;

import com.btoddb.fastpersitentqueue.chronicle.RouteAndSnoop;
import com.btoddb.fastpersitentqueue.chronicle.ChronicleComponentBaseImpl;
import com.btoddb.fastpersitentqueue.chronicle.Config;
import com.btoddb.fastpersitentqueue.chronicle.FpqCatcher;
import com.btoddb.fastpersitentqueue.chronicle.FpqEvent;

import java.util.Collection;


/**
 *
 */
public abstract class CatcherBaseImpl extends ChronicleComponentBaseImpl implements FpqCatcher {
    private RouteAndSnoop router;

    public void init(Config config, RouteAndSnoop router) throws Exception {
        super.init(config);
        this.router = router;
    }

    protected void catchEvents(Collection<FpqEvent> events) {
        router.handleCatcher(getId(), events);
    }
}
