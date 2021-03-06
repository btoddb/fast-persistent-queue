package com.btoddb.fastpersitentqueue.flume;

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
import com.btoddb.fastpersitentqueue.Utils;
import org.apache.flume.Event;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.flume.instrumentation.ChannelCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;


/**
 *
 */
public class FpqTransaction extends BasicTransactionSemantics {
    private static final Logger logger = LoggerFactory.getLogger(FpqTransaction.class);

    private final Fpq fpq;
    private final EventSerializer serializer;
    private final ChannelCounter channelCounter;
    private int eventCount;
    private boolean pushing;

    public FpqTransaction(Fpq fpq, EventSerializer serializer, int capacity, ChannelCounter channelCounter) {
        this.fpq = fpq;
        this.serializer = serializer;
        this.channelCounter = channelCounter;
        fpq.beginTransaction();
    }

    @Override
    protected void doPut(Event event) throws InterruptedException {
        pushing = true;
        eventCount ++;
        channelCounter.incrementEventPutAttemptCount();
        byte[] data = serializer.toBytes(event);
        fpq.push(data);
    }

    @Override
    protected Event doTake() throws InterruptedException {
        pushing = false;
        channelCounter.incrementEventTakeAttemptCount();
        eventCount ++;
        Collection<FpqEntry> entries = fpq.pop(1);
        if (null == entries || entries.isEmpty()) {
            return null;
        }

        return serializer.fromBytes(entries.iterator().next().getData());
    }

    @Override
    protected void doCommit() throws InterruptedException {
        try {
            fpq.commit();
            if (pushing) {
                channelCounter.addToEventPutSuccessCount(eventCount);
            }
            else {
                channelCounter.addToEventTakeSuccessCount(eventCount);
            }
            channelCounter.setChannelSize(fpq.getNumberOfEntries());
        }
        catch (IOException e) {
            Utils.logAndThrow(logger, "exception while committing transaction");
        }
    }

    @Override
    protected void doRollback() throws InterruptedException {
        fpq.rollback();
        channelCounter.setChannelSize(fpq.getNumberOfEntries());
    }
}
