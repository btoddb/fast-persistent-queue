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

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.Map;


/**
 *
 */
public class Chronicle implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Chronicle.class);

    private Config config;
    private boolean canAppEnd;

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
            logger.error("exception while configuring Chronicle - cannot continue", e);
            shutdown();
        }
    }

    // configure the catchers
    // each catcher is wrapped with a CatcherWrapper which handles the
    // catcher callback and applying snoopers
    private void configureCatchers(Config config) throws Exception {
        for (Map.Entry<String, RouteAndSnoop> entry : config.getCatchers().entrySet()) {
            RouteAndSnoop router = entry.getValue();
            router.setChronicle(this);
            initializeComponent(router, entry.getKey());
        }
    }

    // configure the plunkers
    // each plunker is paired with an FPQ within a PlunkerRunner
    // the PlunkerRunner handles the polling of FPQ, TX mgmt, and deliver to plunker
    private void configurePlunkers(Config config) throws Exception {
        for (Map.Entry<String, PlunkerRunner> entry : config.getPlunkers().entrySet()) {
            PlunkerRunner runner = entry.getValue();
            initializeComponent(runner, entry.getKey());
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
     * @param events list of {@link com.btoddb.fastpersitentqueue.chronicle.FpqEvent}s
     */
    public void handleCatcher(String catcherId, Collection<FpqEvent> events) {
        Multimap<PlunkerRunner, FpqEvent> routingMap = LinkedListMultimap.create();

        // divide events by route
        for (FpqEvent event : events) {
            for (FpqRouter router : config.getRouters().values()) {
                PlunkerRunner runner;
                if (null != (runner=router.canRoute(catcherId, event))) {
                    routingMap.put(runner, event);
                }
            }
        }

        if (!routingMap.isEmpty()) {
            for (PlunkerRunner runner : routingMap.keySet()) {
                try {
                    runner.run(routingMap.get(runner));
                }
                catch (Exception e) {
                    Utils.logAndThrow(logger, String.format("exception while handle events from catcher, %s", catcherId), e);
                }
            }
        }
        else {
            config.getErrorHandler().handle(events);
        }
    }

    /**
     * Should be called when finished with chronicle.
     *
     */
    public void shutdown() {
        if (null == config) {
            return;
        }

        if (null != config.getCatchers()) {
            for (RouteAndSnoop routeAndSnoop : config.getCatchers().values()) {
                try {
                    routeAndSnoop.shutdown();
                }
                catch (Exception e) {
                    logger.error("exception while shutting down FPQ catcher, {}", routeAndSnoop.getCatcher().getId(), e);
                }
            }
        }

        if (null != config.getRouters()) {
            for (FpqRouter router : config.getRouters().values()) {
                try {
                    router.shutdown();
                }
                catch (Exception e) {
                    logger.error("exception while shutting down FPQ router, {}", router.getId());
                }
            }
        }

        if (null != config.getPlunkers()) {
            for (PlunkerRunner runner : config.getPlunkers().values()) {
                try {
                    runner.shutdown();
                }
                catch (Exception e) {
                    logger.error("exception while shutting down FPQ plunker, {}", runner.getPlunker().getId(), e);
                }
            }
        }

        canAppEnd = true;
    }

    public void waitUntilStopped() {
        File stopFile = new File(config.getStopFile());
        stopFile.delete();
        while (!canAppEnd) {
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                Thread.interrupted();
            }

            if (stopFile.exists()) {
                stopFile.delete();
                shutdown();
            }
        }
    }

    public Config getConfig() {
        return config;
    }

    /**
     * This is only for the java shutdown hook so we can catch CTRL-C or KILL
     *
     */
    @Override
    public void run() {
        shutdown();
    }

    public static void main(String[] args) throws FileNotFoundException {
        if (args.length != 1) {
            System.out.println();
            System.out.println(String.format("usage: java -cp chronicle.jar %s <chronical.yaml>", Chronicle.class.getName()));
            System.out.println("       (config file is not required to be named 'chronical.yaml')");
            System.out.println();
            System.exit(1);
        }

        Chronicle chronicle = new Chronicle();
        Runtime.getRuntime().addShutdownHook(new Thread(chronicle));

        Config config = Config.create(args[0]);
        chronicle.init(config);


        // we're live!  end the application by CTRL-C, unix 'kill' command, or touching the 'stopFile' file
        // (Note: if you use kill -9 *cannot* be intercepted)

        System.out.println();
        System.out.println("  Chronicle can be killed by touching the file, '" + config.getStopFile() + "'");
        System.out.println();

        chronicle.waitUntilStopped();
    }
}
