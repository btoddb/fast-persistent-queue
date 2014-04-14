package com.btoddb.fastpersitentqueue.flume;

import com.btoddb.fastpersitentqueue.Fpq;
import com.btoddb.fastpersitentqueue.FpqContext;
import com.btoddb.fastpersitentqueue.Utils;
import org.apache.flume.*;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.flume.conf.Configurable;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;
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

    @Override
    protected BasicTransactionSemantics createTransaction() {
        return new FpqTransaction(fpq, fpq.createContext(), eventSerializer, fpq.getMaxTransactionSize());
    }

    @Override
    public void configure(Context context) {
        setMaxJournalFileSize(context.getLong("maxJournalFileSize", 10000000L));
        setNumberOfFlushWorkers(context.getInteger("numberOfFlushWorkers", 4));
        setFlushPeriodInMs(context.getLong("flushPeriodInMs", 10000L));
        setMaxJournalDurationInMs(context.getLong("maxJournalDurationInMs", 30000L));
        setJournalDirectory(new File(context.getString("journalDirectory")));
        setMaxMemorySegmentSizeInBytes(context.getLong("maxMemorySegmentSizeInBytes", 10000000L));
        setPagingDirectory(new File(context.getString("pagingDirectory")));
        setMaxTransactionSize(context.getInteger("transactionCapacity", 2000));
    }

    @Override
    public void start() {
        fpq = new Fpq();
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

    }

    @Override
    public synchronized void stop() {
        if (null != fpq) {
            fpq.shutdown();
        }
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
