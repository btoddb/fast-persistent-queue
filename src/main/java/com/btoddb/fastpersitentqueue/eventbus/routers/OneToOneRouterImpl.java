package com.btoddb.fastpersitentqueue.eventbus.routers;

import com.btoddb.fastpersitentqueue.eventbus.EventBusComponentBaseImpl;
import com.btoddb.fastpersitentqueue.eventbus.FpqRouter;
import com.btoddb.fastpersitentqueue.eventbus.Config;
import com.btoddb.fastpersitentqueue.eventbus.FpqEvent;
import com.btoddb.fastpersitentqueue.eventbus.PlunkerRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Routes all events received by the named {@link com.btoddb.fastpersitentqueue.eventbus.FpqCatcher} to the
 * specified {@link com.btoddb.fastpersitentqueue.eventbus.FpqPlunker}.
 */
public class OneToOneRouterImpl extends EventBusComponentBaseImpl implements FpqRouter {
    private static final Logger logger = LoggerFactory.getLogger(OneToOneRouterImpl.class);

    private String catcher;
    private String plunker;

    @Override
    public void init(Config config) {
        super.init(config);
    }

    @Override
    public void shutdown() {
        // nothing to do yet
    }

    @Override
    public PlunkerRunner canRoute(String catcherId, FpqEvent event) {
        if (catcher.equals(catcherId)) {
            return config.getPlunkers().get(plunker);
        }
        else {
            return null;
        }
    }

    public String getCatcher() {
        return catcher;
    }

    public void setCatcher(String catcher) {
        this.catcher = catcher;
    }

    public String getPlunker() {
        return plunker;
    }

    public void setPlunker(String plunker) {
        this.plunker = plunker;
    }
}
