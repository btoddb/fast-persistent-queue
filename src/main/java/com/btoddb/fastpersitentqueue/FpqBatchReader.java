package com.btoddb.fastpersitentqueue;

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

import com.btoddb.fastpersitentqueue.exceptions.FpqException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;


/**
 * Poll FPQ trying to acquire a batch of events
 */
public class FpqBatchReader implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(FpqBatchReader.class);

    private Fpq fpq;
    private int maxBatchSize;
    private long maxBatchWaitInMs = 3000;
    private long sleepTimeBetweenPopsInMs = 200;
    private FpqBatchCallback callback;

    private volatile boolean stopProcessing;
    private Thread theThread;

    /**
     * Initialize the thread state and start it
     */
    public void init() {
        if (null == fpq) {
            throw new FpqException("FPQ has not been set/initialized - cannot start read thread");
        }

        if (0 == maxBatchSize) {
            maxBatchSize = fpq.getMaxTransactionSize();
        }
        else if (maxBatchSize > fpq.getMaxTransactionSize()) {
            throw new FpqException("maxBatchSize cannot be greater than FPQ.maxTransactionSize - cannot start read thread");
        }
    }

    public void start() {
        theThread = new Thread(this);
        theThread.setName(getClass().getSimpleName());
        theThread.start();
    }

    @Override
    public void run() {
        long waitTimeEnd = calculateWaitEnd();

        while (!stopProcessing) {
            fpq.beginTransaction();
            try {
                // grab a batch - may not be large enough
                Collection<FpqEntry> entries = fpq.pop(maxBatchSize);

                // TODO:BTB - if the maximum entries were popped from a single segment, but less than
                // the batch size request, we will wait the maximum time ('waitTimeEnd') for every pop
                // creating a massive slowdown!!!  Need information about the 'pop' to make a informed
                // decision about how long to wait (if at all) for a full batch!

                // if not large enough and wait duration not exceeded, then rollback and wait some more
                if (null == entries || entries.size() < maxBatchSize) {
                    // if we haven't reached the end of our "wait for full batch" time, then rollback and sleep
                    if (System.currentTimeMillis() <= waitTimeEnd) {
                        logger.debug("did not get a complete batch and wait time not exceeded - no callback");
                        fpq.rollback();
                        Thread.sleep(sleepTimeBetweenPopsInMs);
                        continue;
                    }
                }

                // either we reached the end of our wait time, or we found a full batch

                if (null != entries) {
                    // do callback.  any exceptions cause rollback, otherwise commit
                    callback.available(entries);
                }
                else if (logger.isDebugEnabled()) {
                    logger.debug("event list was empty - no callback");
                }

                fpq.commit();

                // reset wait time
                waitTimeEnd = calculateWaitEnd();
            }
            catch (Exception e) {
                logger.error("exception while reading from FPQ", e);
                fpq.rollback();
                backoffOnPolling();
            }
        }
    }

    private void backoffOnPolling() {
        try {
            Thread.sleep(1000);
        }
        catch (InterruptedException e) {
            Thread.interrupted();
        }
    }

    private long calculateWaitEnd() {
        return System.currentTimeMillis() + maxBatchWaitInMs;
    }

    public Fpq getFpq() {
        return fpq;
    }

    public void setFpq(Fpq fpq) {
        this.fpq = fpq;
    }

    public boolean isStopProcessing() {
        return stopProcessing;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    public FpqBatchCallback getCallback() {
        return callback;
    }

    public void setCallback(FpqBatchCallback callback) {
        this.callback = callback;
    }

    public long getMaxBatchWaitInMs() {
        return maxBatchWaitInMs;
    }

    public void setMaxBatchWaitInMs(long maxBatchWaitInMs) {
        this.maxBatchWaitInMs = maxBatchWaitInMs;
    }

    public long getSleepTimeBetweenPopsInMs() {
        return sleepTimeBetweenPopsInMs;
    }

    public void setSleepTimeBetweenPopsInMs(long sleepTimeBetweenPopsInMs) {
        this.sleepTimeBetweenPopsInMs = sleepTimeBetweenPopsInMs;
    }

    public void shutdown() {
        stopProcessing = true;
    }
}
