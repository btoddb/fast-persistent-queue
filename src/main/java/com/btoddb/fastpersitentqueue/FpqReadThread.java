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
public class FpqReadThread implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(FpqReadThread.class);

    private Fpq fpq;
    private int maxBatchSize;
    private ReadCallback callback;

    private volatile boolean stopProcessing;
    private Thread theThread;

    /**
     * Initialize the thread state and start it
     */
    public void init() {
        if (null != fpq) {
            throw new FpqException("FPQ has not been set/initialized - cannot start read thread");
        }

        if (0 == maxBatchSize) {
            maxBatchSize = fpq.getMaxTransactionSize();
        }
        else if (maxBatchSize > fpq.getMaxTransactionSize()) {
            throw new FpqException("maxBatchSize cannot be greater than FPQ.maxTransactionSize - cannot start read thread");
        }

        theThread = new Thread(this);
        theThread.setName(getClass().getSimpleName());
        theThread.start();
    }

    @Override
    public void run() {
        while (!stopProcessing) {
            fpq.beginTransaction();
            try {
                if (callback.entries(fpq.pop(maxBatchSize))) {
                    fpq.commit();
                }
                else {
                    fpq.rollback();
                }
            }
            catch (Exception e) {
                logger.error("exception while reading from FPQ", e);
                fpq.rollback();
            }
        }
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

    public void setStopProcessing(boolean stopProcessing) {
        this.stopProcessing = stopProcessing;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }
}
