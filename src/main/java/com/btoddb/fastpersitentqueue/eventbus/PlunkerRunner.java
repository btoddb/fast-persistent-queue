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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * Runs the plunker - handling the mundane part of popping from FPQ, etc that all
 * plunkers must do.
 */
public class PlunkerRunner implements EventBusComponent, FpqBatchCallback {
    private static final Logger logger = LoggerFactory.getLogger(PlunkerRunner.class);

    private Config config;
    private FpqBatchReader batchReader;

    private FpqPlunker plunker;
    private Fpq fpq;

    @Override
    public void init(Config config) throws Exception {
        this.config = config;

        plunker.init(config);

        fpq.init();

        batchReader = new FpqBatchReader();
        batchReader.setFpq(fpq);
        batchReader.setCallback(this);
        batchReader.init();
        // see this.available()
        batchReader.start();
    }

    /**
     * Send events to the FPQ associated with the {@link com.btoddb.fastpersitentqueue.eventbus.FpqPlunker}.  Handles
     * all TX management - will rollback if plunker throws any exceptions and then rethrow exception.
     *
     * @param events Collection of {@link com.btoddb.fastpersitentqueue.eventbus.FpqEvent}
     */
    public void run(Collection<FpqEvent> events) {
        Fpq fpq = getFpq();

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
    public void available(Collection<FpqEntry> entries) throws Exception {
        List<FpqEvent> eventList = new ArrayList<FpqEvent>(entries.size());
        for (FpqEntry entry : entries) {
            FpqEvent event = config.getObjectMapper().readValue(entry.getData(), FpqEvent.class);
            eventList.add(event);
        }

        plunker.handle(eventList);
    }

    public void shutdown() {
        try {
            batchReader.shutdown();
        }
        catch (Exception e) {
            logger.error("exception while shutting down FPQ batch reader", e);
        }

        try {
            fpq.shutdown();
        }
        catch (Exception e) {
            logger.error("exception while shutting down FPQ", e);
        }

        try {
            plunker.shutdown();
        }
        catch (Exception e) {
            logger.error("exception while shutting down plunker, {}", plunker.getId(), e);
        }
    }

    @Override
    public String getId() {
        return null;
    }

    @Override
    public void setId(String id) {

    }

    @Override
    public Config getConfig() {
        return null;
    }

    @Override
    public void setConfig(Config config) {

    }

    public FpqPlunker getPlunker() {
        return plunker;
    }

    public void setPlunker(FpqPlunker plunker) {
        this.plunker = plunker;
    }

    public Fpq getFpq() {
        return fpq;
    }

    public void setFpq(Fpq fpq) {
        this.fpq = fpq;
    }
}
