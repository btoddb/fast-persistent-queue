package com.btoddb.fastpersitentqueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;


/**
 *
 */
public class Fpq {
    private static final Logger logger = LoggerFactory.getLogger(Fpq.class);

    private File journalDirectory;
    private JournalMgr journalMgr;

    private InMemorySegmentMgr memoryMgr;

    private File pagingDirectory;
    private long maxMemorySegmentSizeInBytes = 1000000;
    private int maxTransactionSize = 100;
    private int numberOfFlushWorkers = 4;
    private long flushPeriodInMs = 10000;
    private long maxJournalFileSize = 100000000;
    private long maxJournalDurationInMs = 5 * 60 * 1000;

    private AtomicLong entryIdGenerator = new AtomicLong();

    private boolean initializing;

    public void init() throws IOException {
        initializing = true;

        logger.info("initializing FPQ");

        journalMgr = new JournalMgr();
        journalMgr.setDirectory(journalDirectory);
        journalMgr.setNumberOfFlushWorkers(numberOfFlushWorkers);
        journalMgr.setFlushPeriodInMs(flushPeriodInMs);
        journalMgr.setMaxJournalFileSize(maxJournalFileSize);
        journalMgr.setMaxJournalDurationInMs(maxJournalDurationInMs);
        journalMgr.init();

        memoryMgr = new InMemorySegmentMgr();
        memoryMgr.setMaxSegmentSizeInBytes(maxMemorySegmentSizeInBytes);
        memoryMgr.setPagingDirectory(pagingDirectory);
        memoryMgr.init();

        replayJournals();

        initializing = false;
    }

    private void replayJournals() {
//        if (0 < journalMgr.getNumberOfEntries()) {
//            logger.info("replaying {} journal entries", journalMgr.getNumberOfEntries());
//            memoryMgr.clear();
//            JournalMgr.JournalReplayIterable replay = journalMgr.createReplayIterable();
//            for (FpqEntry entry : replay) {
//                memoryMgr.push(entry);
//            }
//        }
    }

    public FpqContext createContext() {
        return new FpqContext(entryIdGenerator, maxTransactionSize);
    }

    public void push(FpqContext context, byte[] event) {
        push(context, Collections.singleton(event));
    }

    public void push(FpqContext context, Collection<byte[]> events) {
        // processes events in the order of the collection's iterator
        // - write events to context.queue - done!  understood no persistence yet
        context.push(events);
    }

    public Collection<FpqEntry> pop(FpqContext context, int size) {
        // - move (up to) context.maxBatchSize events from globalMemoryQueue to context.queue
        // - do not wait for events
        // - return context.queue

        // assumptions
        // - can call 'pop' with same context over and over until context.queue has size context.maxBatchSize

        if (size > maxTransactionSize) {
            throw new FpqException("size of " + size + " exceeds maximum transaction size of " + maxTransactionSize);
        }

        Collection<FpqEntry> entries= memoryMgr.pop(size);

        // at this point, if system crashes, the entries are in the journal files

        // TODO:BTB - however this is not true if system has been previously shutdown and not all of the
        //   entries have been pop'ed.  on shutdown the journal files are drained into memory segments,
        //   which are then serialized to disk (this is done to avoid dupes)

        context.createPoppedEntries(entries);
        return context.getQueue();
    }

    public void commit(FpqContext context) throws IOException {
        // assumptions
        // - fsync thread will persist

        if (context.isPushing()) {
            commitForPush(context);
        }
        else {
            commitForPop(context);
        }

        // - free context.queue
        context.cleanup();
    }

    private void commitForPop(FpqContext context) throws IOException {
        // - context.clearBatch
        // - if commit log file is no longer needed, remove in background work thread

        if (!context.isQueueEmpty()) {
            journalMgr.reportTake(context.getQueue());
        }
    }

    private void commitForPush(FpqContext context) throws IOException {
        // - write events to commit log file (don't fsync)
        // - write events to globalMemoryQueue (for popping)
        // - roll persistent queue

        if (context.isQueueEmpty()) {
            return;
        }

        Collection<FpqEntry> entries = journalMgr.append(context.getQueue());
        memoryMgr.push(entries);
    }

    public void rollback(FpqContext context) {
        if (context.isPushing()) {
            rollbackForPush(context);
        }
        else {
            rollbackForPop(context);
        }
    }

    private void rollbackForPush(FpqContext context) {
        // - free context scoped memory queue
        context.cleanup();
    }

    private void rollbackForPop(FpqContext context) {
        // this one causes the problems with sync between memory and commit log files
        // - mv context.queue to front of globalMemoryQueue (can't move to end.  will screw up deleting of log files)
        // TODO:BTB - if shutdown in progress, can't push back onto queue and pop'ed entries will be lost
        memoryMgr.push(context.getQueue());
        context.cleanup();
    }

    public boolean isEmpty() {
        return 0 == memoryMgr.size();
    }

    public void shutdown() {
        memoryMgr.shutdown();
        journalMgr.shutdown();
    }

    public int getMaxTransactionSize() {
        return maxTransactionSize;
    }

    public void setMaxTransactionSize(int maxTransactionSize) {
        this.maxTransactionSize = maxTransactionSize;
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

    public JournalMgr getJournalMgr() {
        return journalMgr;
    }

    public InMemorySegmentMgr getMemoryMgr() {
        return memoryMgr;
    }

    public void setNumberOfFlushWorkers(int numberOfFlushWorkers) {
        this.numberOfFlushWorkers = numberOfFlushWorkers;
    }

    public int getNumberOfFlushWorkers() {
        return numberOfFlushWorkers;
    }

    public void setFlushPeriodInMs(long flushPeriodInMs) {
        this.flushPeriodInMs = flushPeriodInMs;
    }

    public long getFlushPeriodInMs() {
        return flushPeriodInMs;
    }

    public void setMaxJournalFileSize(long maxJournalFileSize) {
        this.maxJournalFileSize = maxJournalFileSize;
    }

    public long getMaxJournalFileSize() {
        return maxJournalFileSize;
    }

    public void setMaxJournalDurationInMs(long maxJournalDurationInMs) {
        this.maxJournalDurationInMs = maxJournalDurationInMs;
    }

    public long getMaxJournalDurationInMs() {
        return maxJournalDurationInMs;
    }

    public long getJournalsCreated() {
        return journalMgr.getJournalsCreated();
    }

    public long getJournalsRemoved() {
        return journalMgr.getJournalsRemoved();
    }

    public File getPagingDirectory() {
        return pagingDirectory;
    }

    public void setPagingDirectory(File pagingDirectory) {
        this.pagingDirectory = pagingDirectory;
    }

    public long getNumberOfEntries() {
        return memoryMgr.getNumberOfEntries();
    }
}
