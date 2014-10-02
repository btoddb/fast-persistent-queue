package com.btoddb.fastpersitentqueue.chronicle;

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

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;


/**
 *
 */
public class Chronicle {
    private static final Logger logger = LoggerFactory.getLogger(Chronicle.class);

    private Config config;
    private boolean stopProcessing;

    /**
     * Call this method to configure and initialize the bus.
     *
     * @param config initialized {@link Config} object
     * @throws FpqException
     */
    public void init(Config config) throws FpqException {
        this.config = config;

        // TODO:BTB - probably should use a "ready to receive" flag and set after all configuration is 'done'!
        try {
            configurePlunkers(config);
            configureRouters(config);
            configureCatchers(config);
        }
        catch (Exception e) {
            Utils.logAndThrow(logger, "exception while configuring Chronicle - cannot continue", e);
        }
    }

    // configure the catchers
    // each catcher is wrapped with a CatcherWrapper which handles the
    // catcher callback and applying snoopers
    private void configureCatchers(Config config) throws Exception {
        for (Map.Entry<String, CatcherWrapper> entry : config.getCatchers().entrySet()) {
            CatcherWrapper wrapper = entry.getValue();
            if (null == wrapper.getCatcher().getId()) {
                wrapper.getCatcher().setId(entry.getKey());
            }
            wrapper.setChronicle(this);
            wrapper.init(config);
        }
    }

    // configure the plunkers
    // each plunker is paired with an FPQ within a PlunkerRunner
    // the PlunkerRunner handles the polling of FPQ, TX mgmt, and deliver to plunker
    private void configurePlunkers(Config config) throws Exception {
        for (Map.Entry<String, PlunkerRunner> entry : config.getPlunkers().entrySet()) {
            PlunkerRunner runner = entry.getValue();
            initializeComponent(runner, entry.getKey());
            FpqPlunker plunker = runner.getPlunker();
            initializeComponent(plunker, entry.getKey());
        }
    }

    // configure the routers
    private void configureRouters(Config config) throws Exception {
        for (Map.Entry<String, FpqRouter> entry : config.getRouters().entrySet()) {
            initializeComponent(entry.getValue(), entry.getKey());
        }
    }

    private void initializeComponent(ChronicleComponent component, String id) throws Exception {
        if (null == component.getId()) {
            component.setId(id);
        }
        component.init(config);
    }

    /**
     * Should be called by a {@link com.btoddb.fastpersitentqueue.chronicle.FpqCatcher} after receiving events.
     *
     * @param catcherId ID of the catcher for reporting and routing
     * @param eventList list of {@link com.btoddb.fastpersitentqueue.chronicle.FpqEvent}s
     */
    public void handleCatcher(String catcherId, List<FpqEvent> eventList) {
        Multimap<PlunkerRunner, FpqEvent> routingMap = LinkedListMultimap.create();

        // divide events by route
        for (FpqEvent event : eventList) {
            for (FpqRouter router : config.getRouters().values()) {
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
     * Should be called when finished with chronicle.
     *
     */
    public void shutdown() {
        for (CatcherWrapper catcher : config.getCatchers().values()) {
            try {
                catcher.shutdown();
            }
            catch (Exception e) {
                logger.error("exception while shutting down FPQ catcher, {}", catcher.getCatcher().getId(), e);
            }
        }

        for (FpqRouter router : config.getRouters().values()) {
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

    public void waitUntilStopped() {
        while (!stopProcessing) {
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                Thread.interrupted();
            }
        }

        shutdown();
    }

    public boolean isStopProcessing() {
        return stopProcessing;
    }

    public void setStopProcessing(boolean stopProcessing) {
        this.stopProcessing = stopProcessing;
    }

    public Config getConfig() {
        return config;
    }

    public static void main(String[] args) throws FileNotFoundException {
        Config config = Config.create("conf/chronical.yaml");
        Chronicle chronicle = new Chronicle();
        chronicle.init(config);

        // we're live!

        chronicle.waitUntilStopped();

    }
}
