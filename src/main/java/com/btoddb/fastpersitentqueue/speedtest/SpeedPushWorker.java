package com.btoddb.fastpersitentqueue.speedtest;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;


/**
 * 
 */
public class SpeedPushWorker implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(SpeedPushWorker.class);

    private final SpeedTestConfig config;
    private final Fpq fpq;
    private final AtomicLong counter;
    private final AtomicLong pushSum;

    int numberOfEntries = 0;

    public SpeedPushWorker(Fpq fpq, SpeedTestConfig config, AtomicLong counter, AtomicLong pushSum) {
        this.fpq = fpq;
        this.config = config;
        this.counter = counter;
        this.pushSum = pushSum;
    }

    @Override
    public void run() {
        try {
            long start = System.currentTimeMillis();
            long end = start + config.getDurationOfTest() * 1000;
            while (System.currentTimeMillis() < end) {
                if (!fpq.isTransactionActive()) {
                    fpq.beginTransaction();
                }
                long x = counter.getAndIncrement();
                fpq.push(createBody(x));
                pushSum.addAndGet(x);
                numberOfEntries++;

                if (fpq.getCurrentTxSize() >= config.getPushBatchSize()) {
                    fpq.commit();
                }
                if (0 < config.getBatchDelay()) {
                    try {
                        Thread.sleep(config.getBatchDelay());
                    }
                    catch (InterruptedException e) {
                        Thread.interrupted();
                    }
                }
            }
            if (fpq.isTransactionActive()) {
                fpq.commit();
            }
        }
        catch (Throwable e) {
            logger.error("exception while pushing to FPQ", e);
        }
    }

    private byte[] createBody(long x) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[config.getEntrySize()]);
        bb.putLong(x);
        return bb.array();
    }

    public int getNumberOfEntries() {
        return numberOfEntries;
    }
}
