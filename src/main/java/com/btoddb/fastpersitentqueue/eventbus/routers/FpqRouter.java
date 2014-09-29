package com.btoddb.fastpersitentqueue.eventbus.routers;

import com.btoddb.fastpersitentqueue.config.Config;
import com.btoddb.fastpersitentqueue.eventbus.FpqEvent;
import com.btoddb.fastpersitentqueue.eventbus.FpqPlunker;

import java.util.Collection;
import java.util.Map;


/**
 * Created by burrb009 on 9/29/14.
 */
public interface FpqRouter {
    void init(Config config);

    void shutdown();

    void canRoute(String catcherName, FpqEvent event);

    void route(Collection<FpqEvent> events);

    String getId();
    void setId(String id);
}
