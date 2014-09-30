package com.btoddb.fastpersitentqueue.eventbus;

/*
 * #%L
 * fast-persistent-queue
 * %%
 * Copyright (C) 2014 btoddb.com
 * %%
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * #L%
 */

import com.btoddb.fastpersitentqueue.Utils;
import com.btoddb.fastpersitentqueue.exceptions.FpqException;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;


/**
 *
 */
public class EventBus {
    private static final Logger logger = LoggerFactory.getLogger(EventBus.class);

    private Config config;

    /**
     * Call this method to configure and initialize the bus.
     *
     * @param config initialized {@link Config} object
     * @throws FpqException
     */
    public void init(Config config) throws FpqException {
        this.config = config;

        try {
            configurePlunkers(config);
            configureCatchers(config);
            configureRouters(config);
        }
        catch (Exception e) {
            Utils.logAndThrow(logger, "exception while configuring EventBus - cannot continue", e);
        }
    }

    // configure the catchers
    private void configureCatchers(Config config) {
        for (FpqCatcher catcher : config.getCatchers()) {
            catcher.init(config, this);
        }
    }

    // configure the plunkers
    // each plunker is paired with an FPQ within a PlunkerRunner
    // the PlunkerRunner handles the polling of FPQ, TX mgmt, and deliver to plunker
    private void configurePlunkers(Config config) throws IOException {
        for (Map.Entry<String, PlunkerRunner> entry : config.getPlunkers().entrySet()) {
            PlunkerRunner runner = entry.getValue();
            FpqPlunker plunker = runner.getPlunker();
            if (null == plunker.getId()) {
                plunker.setId(entry.getKey());
            }
            runner.init(config);
        }
    }

    // configure the routers
    private void configureRouters(Config config) {
        for (FpqRouter router : config.getRouters()) {
            router.init(config);
        }
    }

    /**
     * Should be called by a {@link com.btoddb.fastpersitentqueue.eventbus.FpqCatcher} after receiving events.
     *
     * @param catcherId ID of the catcher for reporting and routing
     * @param eventList list of {@link com.btoddb.fastpersitentqueue.eventbus.FpqEvent}s
     */
    public void handleCatcher(String catcherId, List<FpqEvent> eventList) {
        Multimap<PlunkerRunner, FpqEvent> routingMap = LinkedListMultimap.create();

        // divide events by route
        for (FpqEvent event : eventList) {
            for (FpqRouter router : config.getRouters()) {
                PlunkerRunner runner;
                if (null != (runner=router.canRoute(catcherId, event))) {
                    routingMap.put(runner, event);
                }
            }
        }

        for (PlunkerRunner runner : routingMap.keySet()) {
            try {
                runner.run(routingMap.get(runner));
            }
            catch (Exception e) {
                Utils.logAndThrow(logger, String.format("exception while handle events from catcher, %s", catcherId), e);
            }
        }
    }

    /**
     * Should be called when finished with the bus.
     *
     */
    public void shutdown() {
        for (FpqCatcher catcher : config.getCatchers()) {
            try {
                catcher.shutdown();
            }
            catch (Exception e) {
                logger.error("exception while shutting down FPQ catcher, {}", catcher.getId(), e);
            }
        }

        for (FpqRouter router : config.getRouters()) {
            try {
                router.shutdown();
            }
            catch (Exception e) {
                logger.error("exception while shutting down FPQ router, {}", router.getId());
            }
        }

        for (PlunkerRunner runner : config.getPlunkers().values()) {
            try {
                runner.shutdown();
            }
            catch (Exception e) {
                logger.error("exception while shutting down FPQ plunker, {}", runner.getPlunker().getId(), e);
            }
        }
    }

    public Config getConfig() {
        return config;
    }

}
