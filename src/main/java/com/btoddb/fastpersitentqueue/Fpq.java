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

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 *
 */
public class Fpq {
    private static final Logger logger = LoggerFactory.getLogger(Fpq.class);

    private File journalDirectory;
    private JournalMgr journalMgr;

    private InMemorySegmentMgr memoryMgr;

    private String queueName;
    private File pagingDirectory;
    private long maxMemorySegmentSizeInBytes = 1000000;
    private int maxTransactionSize = 100;
    private int numberOfFlushWorkers = 4;
    private long flushPeriodInMs = 10000;
    private long maxJournalFileSize = 100000000;
    private long maxJournalDurationInMs = 5 * 60 * 1000;

    private AtomicLong entryIdGenerator = new AtomicLong();
    private long journalEntriesReplayed;
    private JmxMetrics jmxMetrics = new JmxMetrics(this);

    // a map is used only because there isn't a ConcurrentHashSet
    private static final Object MAP_OBJECT = new Object();
    private ConcurrentHashMap<FpqContext, Object> activeContexts = new ConcurrentHashMap<FpqContext, Object>();
    ThreadLocal<FpqContext> threadLocalFpqContext = new ThreadLocal<FpqContext>();

    private ReentrantReadWriteLock shutdownLock = new ReentrantReadWriteLock();
    private volatile boolean initializing;
    private volatile boolean shuttingDown;
    private long waitBeforeKillOnShutdown = 10000;


    public void init() throws IOException {
        initializing = true;

        logger.info("initializing FPQ");

        if (null == queueName) {
            queueName = "queue-name-not-set";
        }

        jmxMetrics.init();

        journalMgr = new JournalMgr();
        journalMgr.setDirectory(journalDirectory);
        journalMgr.setNumberOfFlushWorkers(numberOfFlushWorkers);
        journalMgr.setFlushPeriodInMs(flushPeriodInMs);
        journalMgr.setMaxJournalFileSize(maxJournalFileSize);
        journalMgr.setMaxJournalDurationInMs(maxJournalDurationInMs);
        journalMgr.init();

        memoryMgr = new InMemorySegmentMgr(jmxMetrics);
        memoryMgr.setMaxSegmentSizeInBytes(maxMemorySegmentSizeInBytes);
        memoryMgr.setPagingDirectory(pagingDirectory);
        memoryMgr.init();

        replayJournals();

        initializing = false;
    }

    private void replayJournals() throws IOException {
        JournalMgr.JournalReplayIterable journalReplayer = journalMgr.createReplayIterable();
        for (FpqEntry entry : journalReplayer) {
            if (!memoryMgr.isEntryQueued(entry)) {
                memoryMgr.push(entry);
                journalEntriesReplayed++;
            }
            else {
                logger.trace("entry already in memory segment : " + entry.getId());
            }
        }
    }

    private FpqContext createContext() {
        return new FpqContext(entryIdGenerator, maxTransactionSize);
    }

    private FpqContext contextThrowException() {
        FpqContext context = threadLocalFpqContext.get();
        if (null == context) {
            throw new FpqException("transaction not started - call beginTransaction first");
        }
        return context;
    }

    /**
     *
     */
    public void beginTransaction() {
        checkInitializing();
        checkShutdown();

        if (isTransactionActive()) {
            throw new FpqException("transaction already started on this thread - must commit/rollback before starting another");
        }
        FpqContext context = createContext();
        threadLocalFpqContext.set(context);
        activeContexts.put(context, MAP_OBJECT);
    }

    // for junits
    FpqContext getContext() {
        return contextThrowException();
    }

    private void cleanupTransaction(FpqContext context) {
        // - free context.queue
        context.cleanup();
        activeContexts.remove(context);
        threadLocalFpqContext.remove();
    }

    /**
     *
     * @return
     */
    public int getCurrentTxSize() {
        return contextThrowException().size();
    }

    /**
     *
     * @return
     */
    public boolean isTransactionActive() {
        return null != threadLocalFpqContext.get();
    }

    /**
     *
     * @param event
     */
    public void push(byte[] event) {
        push(Collections.singleton(event));
    }

    /**
     *
     * @param events
     */
    public void push(Collection<byte[]> events) {
        checkInitializing();
        checkShutdown();

        // processes events in the order of the collection's iterator
        // - write events to context.queue - done!  understood no persistence yet
        contextThrowException().push(events);
    }

    /**
     *
     * @param size
     * @return
     */
    public Collection<FpqEntry> pop(int size) {
        checkInitializing();
        checkShutdown();

        // - move (up to) context.maxBatchSize events from globalMemoryQueue to context.queue
        // - do not wait for events
        // - return context.queue

        // assumptions
        // - can call 'pop' with same context over and over until context.queue has size context.maxBatchSize

        FpqContext context = contextThrowException();

        if (size > maxTransactionSize) {
            throw new FpqException("size of " + size + " exceeds maximum transaction size of " + maxTransactionSize);
        }

        Collection<FpqEntry> entries= memoryMgr.pop(size);

        // at this point, if system crashes, the entries are in the journal files

        // TODO:BTB - however this is not true if system has been previously shutdown and not all of the
        //   entries have been pop'ed.  on shutdown the journal files are drained into memory segments,
        //   which are then serialized to disk (this is done to avoid dupes)

        context.createPoppedEntries(entries);
        return entries;
    }

    /**
     *
     * @throws IOException
     */
    public void commit() throws IOException {
        // assumptions
        // - fsync thread will persist
        shutdownLock.readLock().lock();
        try {
            FpqContext context = contextThrowException();

            if (context.isPushing()) {
                commitForPush(context);
            }
            else {
                commitForPop(context);
            }

            cleanupTransaction(context);
        }
        finally {
            shutdownLock.readLock().unlock();
        }
    }

    private void commitForPop(FpqContext context) throws IOException {
        // - context.clearBatch
        // - if commit log file is no longer needed, remove in background work thread

        if (!context.isQueueEmpty()) {
            journalMgr.reportTake(context.getQueue());
            jmxMetrics.incrementPops(context.size());
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
        jmxMetrics.incrementPushes(entries.size());
    }

    /**
     *
     */
    public void rollback() {
        shutdownLock.readLock().lock();
        try {
            FpqContext context = contextThrowException();
            rollbackInternal(context);
        }
        finally {
            shutdownLock.readLock().unlock();
        }
    }

    private void rollbackInternal(FpqContext context) {
        if (context.isPushing()) {
            rollbackForPush(context);
        }
        else {
            rollbackForPop(context);
        }

        cleanupTransaction(context);
    }

    private void rollbackForPush(FpqContext context) {
        // - free context scoped memory queue
        context.cleanup();
    }

    private void rollbackForPop(FpqContext context) {
        // this one causes the problems with sync between memory and commit log files
        // - mv context.queue to front of globalMemoryQueue (can't move to end.  will screw up deleting of log files)
        if (!context.isQueueEmpty()) {
            memoryMgr.push(context.getQueue());
        }
        context.cleanup();
    }

    /**
     *
     * @return
     */
    public boolean isEmpty() {
        return 0 == memoryMgr.size();
    }

    public void shutdown() {
        // stop new transactions, and opertions on FPQ
        shuttingDown = true;

        // give customer clients to to react by committing or rolling back
        long endTime = System.currentTimeMillis() + waitBeforeKillOnShutdown;
        while (!activeContexts.isEmpty() && System.currentTimeMillis() < endTime) {
            try {
                Thread.sleep(500);
            }
            catch (InterruptedException e) {
                // ignore
                Thread.interrupted();
            }
        }

        shutdownLock.writeLock().lock();
        try {
            // any pop'ed entries in progress will be preserved by journals
            for (FpqContext context : activeContexts.keySet()) {
                cleanupTransaction(context);
            }

            if (null != memoryMgr) {
                memoryMgr.shutdown();
            }
            if (null != journalMgr) {
                journalMgr.shutdown();
            }
        }
        finally {
            shutdownLock.writeLock().unlock();
        }
    }

    private void checkInitializing() {
        if (initializing) {
            throw new FpqException("FPQ still initializing - can't perform operation");
        }
    }

    private void checkShutdown() {
        if (shuttingDown) {
            throw new FpqException("FPQ shutting down - can't perform operation");
        }
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

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public long getJournalFilesReplayed() {
        return journalMgr.getJournalsLoadedAtStartup();
    }

    public long getJournalEntriesReplayed() {
        return journalEntriesReplayed;
    }

    JmxMetrics getJmxMetrics() {
        return jmxMetrics;
    }
}
