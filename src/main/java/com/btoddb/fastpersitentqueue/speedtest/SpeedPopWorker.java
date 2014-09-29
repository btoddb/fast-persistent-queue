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
import com.btoddb.fastpersitentqueue.FpqEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;


/**
 * 
 */
public class SpeedPopWorker implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(SpeedPopWorker.class);

    private final Fpq fpq;
    private final SpeedTestConfig config;
    private final AtomicLong sum;

    int numberOfEntries = 0;
    private boolean stopWhenQueueEmpty = false;

    public SpeedPopWorker(Fpq fpq, SpeedTestConfig config, AtomicLong sum) {
        this.fpq = fpq;
        this.config = config;
        this.sum = sum;
    }

    @Override
    public void run() {
        try {
            while (!stopWhenQueueEmpty || !fpq.isEmpty()) {
                Thread.sleep(10);
                Collection<FpqEntry> entries;
                do {
                    fpq.beginTransaction();
                    entries = fpq.pop(config.getPopBatchSize());
                    if (null != entries && !entries.isEmpty()) {
                        numberOfEntries += entries.size();
                        for (FpqEntry entry : entries) {
                            ByteBuffer bb = ByteBuffer.wrap(entry.getData());
                            sum.addAndGet(bb.getLong());
                        }
                        entries.clear();
                    }
                    fpq.commit();
                } while (null != entries && !entries.isEmpty());
            }
        }
        catch (Throwable e) {
            logger.error("exception while popping from FPQ", e);
            e.printStackTrace();
        }
    }

    public int getNumberOfEntries() {
        return numberOfEntries;
    }

    public void stopWhenQueueEmpty() {
        stopWhenQueueEmpty = true;
    }
}
