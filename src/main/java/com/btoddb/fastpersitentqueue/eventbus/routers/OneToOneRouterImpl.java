package com.btoddb.fastpersitentqueue.eventbus.routers;

import com.btoddb.fastpersitentqueue.Fpq;
import com.btoddb.fastpersitentqueue.Utils;
import com.btoddb.fastpersitentqueue.config.Config;
import com.btoddb.fastpersitentqueue.eventbus.FpqEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;


/**
 *
 */
public class OneToOneRouterImpl implements FpqRouter {
    private static final Logger logger = LoggerFactory.getLogger(OneToOneRouterImpl.class);

    private Config config;

    private String catcher;
    private String plunker;

    @Override
    public void init(Config config) {
        this.config = config;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public void canRoute(String catcherName, FpqEvent event) {

    }

    @Override
    public void route(Collection<FpqEvent> events) {
        Fpq fpq = config.getPlunkers().get(plunker).getFpq();
        fpq.beginTransaction();
        try {
            for (FpqEvent event : events) {
                fpq.push(config.getObjectMapper().writeValueAsBytes(event));
            }
            fpq.commit();
        }
        catch (Exception e) {
            Utils.logAndThrow(logger, String.format("exception while routing events to plunker, %s", plunker), e);
        }
        finally {
            if (fpq.isTransactionActive()) {
                fpq.rollback();
            }
        }
    }

    @Override
    public String getId() {
        return null;
    }

    @Override
    public void setId(String id) {

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
