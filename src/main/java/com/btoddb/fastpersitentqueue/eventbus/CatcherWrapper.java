package com.btoddb.fastpersitentqueue.eventbus;

import com.btoddb.fastpersitentqueue.eventbus.wiretaps.Snooper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 *
 */
public class CatcherWrapper {
    private static final Logger logger = LoggerFactory.getLogger(CatcherWrapper.class);

    private FpqCatcher catcher;
    private Map<String, Snooper> snoopers;

    private EventBus eventBus;

    public void init(Config config, EventBus eventBus) {
        this.eventBus = eventBus;

        if (null != snoopers) {
            for (Map.Entry<String, Snooper> entry : snoopers.entrySet()) {
                entry.getValue().setId(entry.getKey());
                entry.getValue().init(config);
            }
        }
        else {
            snoopers = Collections.emptyMap();
        }

        // init the catcher last, after all snoopers are ready
        catcher.init(config, this);
    }

    public void shutdown() {
        try {
            catcher.shutdown();
        }
        catch (Exception e) {
            logger.error("exception while shutting down catcher, {}", catcher.getId());
        }

        if (null != snoopers) {
            for (Snooper snooper : snoopers.values()) {
                try {
                    snooper.shutdown();
                }
                catch (Exception e) {
                    logger.error("exception while shutting down snooper, {}", snooper.getId());
                }
            }
        }
    }

    public void handleCatcher(String catcherId, List<FpqEvent> eventList) {
        for (FpqEvent event : eventList) {
            for (Snooper snooper : snoopers.values()) {
                snooper.tap(event);
            }
        }
        eventBus.handleCatcher(catcherId, eventList);
    }

    public FpqCatcher getCatcher() {
        return catcher;
    }

    public void setCatcher(FpqCatcher catcher) {
        this.catcher = catcher;
    }

    public Map<String, Snooper> getSnoopers() {
        return snoopers;
    }

    public void setSnoopers(Map<String, Snooper> snoopers) {
        this.snoopers = snoopers;
    }
}
