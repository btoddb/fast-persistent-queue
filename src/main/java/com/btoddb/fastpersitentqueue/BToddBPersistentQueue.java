package com.btoddb.fastpersitentqueue;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;


/**
 *
 */
public class BToddBPersistentQueue {
    private File journalDirectory;
    private JournalFileMgr journalFileMgr;

    private GlobalMemoryQueue queue;

    private long memoryQueueMaxSize = 100;
    private int maxTransactionSize = 100;
    private int numberOfFlushWorkers = 4;
    private int flushPeriodInMs = 10000;
    private int maxJournalFileSize = 100000000;
    private int maxJournalDurationInMs = 5 * 60 * 1000;

    public void init() throws IOException {
        journalFileMgr = new JournalFileMgr();
        journalFileMgr.setJournalDirectory(journalDirectory);
        journalFileMgr.setNumberOfFlushWorkers(numberOfFlushWorkers);
        journalFileMgr.setFlushPeriodInMs(flushPeriodInMs);
        journalFileMgr.setMaxJournalFileSize(maxJournalFileSize);
        journalFileMgr.setMaxJournalDurationInMs(maxJournalDurationInMs);
        journalFileMgr.init();

        queue = new GlobalMemoryQueue();
        queue.setMaxSize(memoryQueueMaxSize);
    }

    public BToddBContext createContext() {
        return new BToddBContext(maxTransactionSize);
    }

    public void push(BToddBContext context, byte[] event) {
        push(context, Collections.singleton(event));
    }

    public void push(BToddBContext context, Collection<byte[]> events) {
        // processes events in the order of the collection's iterator
        // - write events to context.queue - done!  understood no persistence yet

        context.push(events);
    }

    public Collection<Entry> pop(BToddBContext context, int size) {
        // - move (up to) context.maxBatchSize events from globalMemoryQueue to context.queue
        // - do not wait for events
        // - return context.queue

        // assumptions
        // - can call 'pop' with same context over and over until context.queue has size context.maxBatchSize

        if (size > maxTransactionSize) {
            throw new BToddBException("size of " + size + " exceeds maximum transaction size of " + maxTransactionSize);
        }

        Collection<Entry> entries=queue.pop(size);
        context.createPoppedEntries(entries);
        return context.getQueue();
    }

    public void commit(BToddBContext context) throws IOException {
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

    private void commitForPop(BToddBContext context) throws IOException {
        // - context.clearBatch
        // - if commit log file is no longer needed, remove in background work thread

        journalFileMgr.reportTake(context.getQueue());
    }

    private void commitForPush(BToddBContext context) throws IOException {
        // - write events to commit log file (don't fsync)
        // - write events to globalMemoryQueue (for popping)
        // - roll persistent queue

        Collection<Entry> entries = journalFileMgr.append(context.getQueue());
        queue.push(entries);
    }

    private void rollbackForPush(BToddBContext context) {
        // - free context scoped memory queue
    }

    private void rollbackForPop(BToddBContext context) {
        // this one causes the problems with sync between memory and commit log files
        // - mv context.queue to front of globalMemoryQueue (can't move to end.  will screw up deleting of log files)
    }

    public boolean isEmpty() {
        return 0 == queue.size();
    }

    public void shutdown() {
        journalFileMgr.shutdown();
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

    public long getMemoryQueueMaxSize() {
        return memoryQueueMaxSize;
    }

    public void setMemoryQueueMaxSize(long memoryQueueMaxSize) {
        this.memoryQueueMaxSize = memoryQueueMaxSize;
    }

    public JournalFileMgr getJournalFileMgr() {
        return journalFileMgr;
    }

    public GlobalMemoryQueue getQueue() {
        return queue;
    }

    public void setNumberOfFlushWorkers(int numberOfFlushWorkers) {
        this.numberOfFlushWorkers = numberOfFlushWorkers;
    }

    public int getNumberOfFlushWorkers() {
        return numberOfFlushWorkers;
    }

    public void setFlushPeriodInMs(int flushPeriodInMs) {
        this.flushPeriodInMs = flushPeriodInMs;
    }

    public int getFlushPeriodInMs() {
        return flushPeriodInMs;
    }

    public void setMaxJournalFileSize(int maxJournalFileSize) {
        this.maxJournalFileSize = maxJournalFileSize;
    }

    public int getMaxJournalFileSize() {
        return maxJournalFileSize;
    }

    public void setMaxJournalDurationInMs(int maxJournalDurationInMs) {
        this.maxJournalDurationInMs = maxJournalDurationInMs;
    }

    public int getMaxJournalDurationInMs() {
        return maxJournalDurationInMs;
    }

    public long getJournalsCreated() {
        return journalFileMgr.getJournalsCreated();
    }
    public long getJournalsRemoved() {
        return journalFileMgr.getJournalsRemoved();
    }
}
