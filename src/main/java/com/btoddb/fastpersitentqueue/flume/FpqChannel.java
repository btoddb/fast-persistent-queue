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
import com.btoddb.fastpersitentqueue.Utils;
import org.apache.flume.*;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.flume.instrumentation.ChannelCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;


/**
 *
 */
public class FpqChannel extends BasicChannelSemantics {
    private static final Logger logger = LoggerFactory.getLogger(FpqChannel.class);

    private EventSerializer eventSerializer = new EventSerializer();
    private Fpq fpq;

    private long maxJournalFileSize;
    private int numberOfFlushWorkers;
    private long flushPeriodInMs;
    private long maxJournalDurationInMs;
    private File journalDirectory;
    private long maxMemorySegmentSizeInBytes;
    private File pagingDirectory;
    private int maxTransactionSize;

    private ChannelCounter channelCounter;

    @Override
    protected BasicTransactionSemantics createTransaction() {
        return new FpqTransaction(fpq, eventSerializer, fpq.getMaxTransactionSize(), channelCounter);
    }

    @Override
    public void configure(Context context) {
        super.configure(context);

        setMaxJournalFileSize(context.getLong("maxJournalFileSize", 10000000L));
        setNumberOfFlushWorkers(context.getInteger("numberOfFlushWorkers", 4));
        setFlushPeriodInMs(context.getLong("flushPeriodInMs", 10000L));
        setMaxJournalDurationInMs(context.getLong("maxJournalDurationInMs", 30000L));
        setJournalDirectory(new File(context.getString("journalDirectory")));
        setMaxMemorySegmentSizeInBytes(context.getLong("maxMemorySegmentSizeInBytes", 10000000L));
        setPagingDirectory(new File(context.getString("pagingDirectory")));
        setMaxTransactionSize(context.getInteger("transactionCapacity", 2000));

        if (channelCounter == null) {
            channelCounter = new ChannelCounter(getName());
        }
    }

    @Override
    public void start() {
        fpq = new Fpq();
        fpq.setQueueName("fpq-flume-"+getName());
        fpq.setMaxJournalFileSize(maxJournalFileSize);
        fpq.setNumberOfFlushWorkers(numberOfFlushWorkers);
        fpq.setFlushPeriodInMs(flushPeriodInMs);
        fpq.setMaxJournalDurationInMs(maxJournalDurationInMs);
        fpq.setJournalDirectory(journalDirectory);
        fpq.setMaxMemorySegmentSizeInBytes(maxMemorySegmentSizeInBytes);
        fpq.setPagingDirectory(pagingDirectory);
        fpq.setMaxTransactionSize(maxTransactionSize);

        try {
            fpq.init();
        }
        catch (IOException e) {
            Utils.logAndThrow(logger, "exception while starting FPQ", e);
        }

        if (channelCounter == null) {
            channelCounter = new ChannelCounter(getName());
        }

        channelCounter.start();
        channelCounter.setChannelSize(fpq.getNumberOfEntries());
        channelCounter.setChannelCapacity(0);

        super.start();
    }

    @Override
    public synchronized void stop() {
        if (null != fpq) {
            fpq.shutdown();
        }
        if (null != channelCounter) {
            channelCounter.setChannelSize(fpq.getNumberOfEntries());
            channelCounter.stop();
        }

        super.stop();
    }

    public boolean isEmpty() {
        return fpq.isEmpty();
    }

    public long getMaxJournalFileSize() {
        return maxJournalFileSize;
    }

    public void setMaxJournalFileSize(long maxJournalFileSize) {
        this.maxJournalFileSize = maxJournalFileSize;
    }

    public int getNumberOfFlushWorkers() {
        return numberOfFlushWorkers;
    }

    public void setNumberOfFlushWorkers(int numberOfFlushWorkers) {
        this.numberOfFlushWorkers = numberOfFlushWorkers;
    }

    public long getFlushPeriodInMs() {
        return flushPeriodInMs;
    }

    public void setFlushPeriodInMs(long flushPeriodInMs) {
        this.flushPeriodInMs = flushPeriodInMs;
    }

    public long getMaxJournalDurationInMs() {
        return maxJournalDurationInMs;
    }

    public void setMaxJournalDurationInMs(long maxJournalDurationInMs) {
        this.maxJournalDurationInMs = maxJournalDurationInMs;
    }

    public File getJournalDirectory() {
        return journalDirectory;
    }

    public void setJournalDirectory(File journalDirectory) {
        this.journalDirectory = journalDirectory;
    }

    public long getMaxMemorySegmentSizeInBytes() {
        return maxMemorySegmentSizeInBytes;
    }

    public void setMaxMemorySegmentSizeInBytes(long maxMemorySegmentSizeInBytes) {
        this.maxMemorySegmentSizeInBytes = maxMemorySegmentSizeInBytes;
    }

    public File getPagingDirectory() {
        return pagingDirectory;
    }

    public void setPagingDirectory(File pagingDirectory) {
        this.pagingDirectory = pagingDirectory;
    }

    public int getMaxTransactionSize() {
        return maxTransactionSize;
    }

    public void setMaxTransactionSize(int maxTransactionSize) {
        this.maxTransactionSize = maxTransactionSize;
    }

}
