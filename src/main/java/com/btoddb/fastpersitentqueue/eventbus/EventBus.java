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

import com.btoddb.fastpersitentqueue.Fpq;
import com.btoddb.fastpersitentqueue.FpqBatchCallback;
import com.btoddb.fastpersitentqueue.FpqBatchReader;
import com.btoddb.fastpersitentqueue.FpqEntry;
import com.btoddb.fastpersitentqueue.Utils;
import com.btoddb.fastpersitentqueue.config.Config;
import com.btoddb.fastpersitentqueue.exceptions.FpqException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 *
 */
public class EventBus implements FpqBatchCallback {
    private static final Logger logger = LoggerFactory.getLogger(EventBus.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private Fpq fpq;
    private FpqBatchReader batchReader;
    private Config config;

    public void init(Config config) throws FpqException {
        this.config = config;
        fpq = new Fpq();
        try {
            fpq.init(config);
        }
        catch (Exception e) {
            Utils.logAndThrow(logger, "exception while initializing FPQ - cannot continue", e);
        }

        configurePlunkers(config);
        configureCatchers(config);

        batchReader = new FpqBatchReader();
        batchReader.setFpq(fpq);
        batchReader.setCallback(this);
        batchReader.init();
    }

    private void configurePlunkers(Config config) {
        for (FpqPlunker plunker : config.getPlunkers()) {
            plunker.init();
        }

    }

    private void configureCatchers(Config config) {
        for (FpqCatcher catcher : config.getCatchers()) {
            catcher.init(fpq);
        }
    }

    public void start() {
        // see this.available()
        batchReader.start();
    }

    @Override
    public void available(Collection<FpqEntry> entries) throws Exception {
        List<FpqEvent> eventList = new ArrayList<FpqEvent>(entries.size());
        for (FpqEntry entry : entries) {
            FpqEvent event = objectMapper.readValue(entry.getData(), FpqEvent.class);
            eventList.add(event);
        }

        config.getPlunkers().iterator().next().handle(eventList);
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

        try {
            fpq.shutdown();
        }
        catch (Exception e) {
            logger.error("exception while shutting down FPQ", e);
        }

        for (FpqPlunker plunker : config.getPlunkers()) {
            try {
                plunker.shutdown();
            }
            catch (Exception e) {
                logger.error("exception while shutting down FPQ plunker, {}", plunker.getId(), e);
            }
        }

        try {
            batchReader.shutdown();
        }
        catch (Exception e) {
            logger.error("exception while shutting down FPQ batch reader", e);
        }
    }
}
