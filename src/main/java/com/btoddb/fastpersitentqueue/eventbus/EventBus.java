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
import com.btoddb.fastpersitentqueue.config.Config;
import com.btoddb.fastpersitentqueue.eventbus.routers.FpqRouter;
import com.btoddb.fastpersitentqueue.exceptions.FpqException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;


/**
 *
 */
public class EventBus {
    private static final Logger logger = LoggerFactory.getLogger(EventBus.class);


//    private Fpq fpq;
    private Config config;

    public void init(Config config) throws FpqException {
        this.config = config;

        configurePlunkers(config);
        configureCatchers(config);
        configureRouters(config);
    }

    private void configureCatchers(Config config) {
        for (FpqCatcher catcher : config.getCatchers()) {
            catcher.init(config, this);
        }
    }

    private void configurePlunkers(Config config) {
        for (Map.Entry<String, FpqPlunker> entry : config.getPlunkers().entrySet()) {
            FpqPlunker plunker = entry.getValue();
            if (null == plunker.getId()) {
                plunker.setId(entry.getKey());
            }
            plunker.init(config);
        }
    }

    private void configureRouters(Config config) {
        for (FpqRouter router : config.getRouters()) {
            router.init(config);
        }
    }

//    public void start() {
//    }

    public void handleCatcher(String catcherId, List<FpqEvent> eventList) {
        try {
            FpqRouter router = config.getRouters().iterator().next();
            router.route(eventList);
        }
        catch (Exception e) {
            Utils.logAndThrow(logger, String.format("exception while handle events from catcher, %s", catcherId), e);
        }

    }
    public Config getConfig() {
        return config;
    }

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

        for (FpqPlunker plunker : config.getPlunkers().values()) {
            try {
                plunker.shutdown();
            }
            catch (Exception e) {
                logger.error("exception while shutting down FPQ plunker, {}", plunker.getId(), e);
            }
        }
    }
}
