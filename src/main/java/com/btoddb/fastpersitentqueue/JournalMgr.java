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
import com.eaio.uuid.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 *
 */
public class JournalMgr {
    private static final Logger logger = LoggerFactory.getLogger(JournalMgr.class);

    private File directory;
    private long flushPeriodInMs = 10000;
    private int numberOfFlushWorkers = 4;
    private int numberOfGeneralWorkers = 2;
    private long maxJournalFileSize = 100000000L;
    private long maxJournalDurationInMs = (10 * 60 * 1000); // 10 minutes

    private volatile boolean shutdownInProgress;
    private long journalsLoadedAtStartup;
    private AtomicLong journalsCreated = new AtomicLong();
    private AtomicLong journalsRemoved = new AtomicLong();
    private AtomicLong numberOfEntries = new AtomicLong();

    private volatile JournalDescriptor currentJournalDescriptor;

    ReentrantReadWriteLock journalLock = new ReentrantReadWriteLock();
    private TreeMap<UUID, JournalDescriptor> journalIdMap = new TreeMap<UUID, JournalDescriptor>(new Comparator<UUID>() {
        @Override
        public int compare(UUID uuid, UUID uuid2) {
            return uuid.compareTo(uuid2);
        }
    });

    private ScheduledThreadPoolExecutor flushExec;
    private ExecutorService generalExec;

    /**
     *
     * @throws IOException
     */
    public void init() throws IOException {
        flushExec = new ScheduledThreadPoolExecutor(numberOfFlushWorkers,
                                                    new ThreadFactory() {
                                                        @Override
                                                        public Thread newThread(Runnable runnable) {
                                                            Thread t = new Thread(runnable);
                                                            t.setName("FPQ-FSync");
                                                            return t;
                                                        }
                                                    });
        generalExec = Executors.newFixedThreadPool(numberOfGeneralWorkers,
                                                    new ThreadFactory() {
                                                        @Override
                                                        public Thread newThread(Runnable runnable) {
                                                            Thread t = new Thread(runnable);
                                                            t.setName("FPQ-GeneralWork");
                                                            return t;
                                                        }
                                                    });

        prepareJournaling();

        if (journalIdMap.isEmpty()) {
            currentJournalDescriptor = createAndAddNewJournal();
        }
        else {
            currentJournalDescriptor = journalIdMap.lastEntry().getValue();
        }
    }

    private void prepareJournaling() throws IOException {
        FileUtils.forceMkdir(directory);
        Collection<File> files = FileUtils.listFiles(directory, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
        if (null == files || 0 == files.size()) {
            logger.info("no previous journal files found");
            return;
        }

        logger.info("loading journal descriptors");
        numberOfEntries.set(0);
        for (File f : files) {
            JournalFile jf = new JournalFile(f);
            jf.initForReading();
            jf.close();

            JournalDescriptor jd = new JournalDescriptor(jf);
            jd.setWritingFinished(true);
            jd.adjustEntryCount(jf.getNumberOfEntries());
            journalIdMap.put(jd.getId(), jd);
            numberOfEntries.addAndGet(jf.getNumberOfEntries());
            logger.info("loaded descriptor, {}, with {} entries", jd.getId(), jd.getNumberOfUnconsumedEntries());
            journalsLoadedAtStartup ++;
        }

        logger.info("completed journal descriptor loading.  found a total of {} entries", numberOfEntries.get());
    }

    public JournalReplayIterable createReplayIterable() throws IOException {
        return new JournalReplayIterable();
    }

    private JournalDescriptor createAndAddNewJournal() throws IOException {
        UUID uid = new UUID();
        String fn = createNewJournalName(uid.toString());
        return addNewJournalToIdMap(uid, fn);
    }

    private JournalDescriptor addNewJournalToIdMap(UUID id, String fn) throws IOException {
        JournalFile jf = new JournalFile(new File(directory, fn));
        jf.initForWriting(id);

        // this could possibly create a race, if the flushPeriodInMs is very very low ;)
        ScheduledFuture future = flushExec.scheduleWithFixedDelay(new FlushRunner(jf), flushPeriodInMs, flushPeriodInMs, TimeUnit.MILLISECONDS);
        JournalDescriptor jd = new JournalDescriptor(id, jf, future);
        journalLock.writeLock().lock();
        try {
            journalIdMap.put(id, jd);
        }
        finally {
            journalLock.writeLock().unlock();
        }

        journalsCreated.incrementAndGet();
        logger.debug("new journal created : {}", jd.getId());
        return jd;
    }

    public FpqEntry append(FpqEntry entry) throws IOException {
        append(Collections.singleton(entry));
        return entry;
    }

    public Collection<FpqEntry> append(Collection<FpqEntry> events) throws IOException {
        if (shutdownInProgress) {
            throw new FpqException("FPQ has been shutdown or is in progress, cannot append");
        }

        // TODO:BTB - optimize this by calling file.append() with collection of data
        List<FpqEntry> entryList = new ArrayList<FpqEntry>(events.size());
        for (FpqEntry entry : events) {
            // TODO:BTB - could use multiple journal files to prevent lock contention - each thread in an "append" pool gets one?
            boolean appended = false;
            while (!appended) {
                JournalDescriptor desc = getCurrentJournalDescriptor();
                if (null == desc) {
                    Utils.logAndThrow(logger, String.format("no current journal descriptor.  did you call %s.init()?", this.getClass().getSimpleName()));
                }
                synchronized (desc) {
                    if (!desc.isWritingFinished()) {
                        desc.getFile().append(entry);
                        entry.setJournalId(desc.getId());
                        entryList.add(entry);

                        if (0 >= desc.getStartTime()) {
                            desc.setStartTime(System.currentTimeMillis());
                        }
                        desc.adjustEntryCount(1);

                        rollJournalIfNeeded();
                        appended = true;
                    }
                }
            }
        }

        numberOfEntries.addAndGet(entryList.size());

        return entryList;
    }

    // this method must be synchronized from above
    private void rollJournalIfNeeded() throws IOException {
        if ( !currentJournalDescriptor.isAnyWritesHappened()) {
            return;
        }

        long fileLength = currentJournalDescriptor.getFile().getFilePosition();
        if (fileLength >= maxJournalFileSize ||
                (0 < fileLength && (currentJournalDescriptor.getStartTime()+maxJournalDurationInMs) < System.currentTimeMillis())) {
            currentJournalDescriptor.setWritingFinished(true);
            currentJournalDescriptor.getFuture().cancel(false);
            currentJournalDescriptor.getFile().close();
            currentJournalDescriptor = createAndAddNewJournal();
        }

    }

    public void reportTake(FpqEntry entry) throws IOException {
        reportTake(Collections.singleton(entry));
    }

    public void reportTake(Collection<FpqEntry> entries) throws IOException {
        if (shutdownInProgress) {
            throw new FpqException("FPQ has been shutdown or is in progress, cannot report TAKE");
        }

        if (null == entries || entries.isEmpty()) {
            logger.debug("provided null or empty collection - ignoring");
            return;
        }

        Map<UUID, Integer> journalIds = new HashMap<UUID, Integer>();
        for (FpqEntry entry : entries) {
            Integer count = journalIds.get(entry.getJournalId());
            if (null == count) {
                count = 0;
            }
            journalIds.put(entry.getJournalId(), count + 1);
        }

        for (Map.Entry<UUID, Integer> entry : journalIds.entrySet()) {
            journalLock.readLock().lock();
            JournalDescriptor desc;
            try {
                desc = journalIdMap.get(entry.getKey());
            }
            finally {
                journalLock.readLock().unlock();
            }

            if (null == desc) {
                logger.error("illegal state - reported consumption of journal entry, but journal descriptor, {}, doesn't exist!", entry.getKey());
                continue;
            }

            long remaining = desc.adjustEntryCount(-entry.getValue());

            // this is therad-safe, because only one thread can decrement down to zero when isWritingFinished
            if (0 == remaining && desc.isWritingFinished()) {
                submitJournalRemoval(desc);
            }
        }
    }

    private void submitJournalRemoval(final JournalDescriptor desc) {
        logger.debug("submitting journal, {}, for removal", desc.getId());
        generalExec.submit(new Runnable() {
            @Override
            public void run() {
                removeJournal(desc);
            }
        });
    }

    private void removeJournal(JournalDescriptor desc) {
        journalLock.writeLock().lock();
        try {
            journalIdMap.remove(desc.getId());
            try {
                FileUtils.forceDelete(desc.getFile().getFile());
            }
            catch (IOException e) {
                logger.error("could not delete journal file, {} - will not try again", desc.getFile().getFile().getAbsolutePath());
            }
        }
        finally {
            journalLock.writeLock().unlock();
        }

        numberOfEntries.addAndGet(-desc.getFile().getNumberOfEntries());
        journalsRemoved.incrementAndGet();
        logger.debug("journal, {}, removed", desc.getId());
    }

    public void shutdown() {
        shutdownInProgress = true;
        if (null != flushExec) {
            flushExec.shutdown();
            try {
                flushExec.awaitTermination(60, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                Thread.interrupted();
                // ignore
            }
        }
        if (null != generalExec) {
            generalExec.shutdown();
            try {
                generalExec.awaitTermination(60, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                Thread.interrupted();
                // ignore
            }
        }

        // for journals that are completely popped, but not removed (because a client still could have pushed)
        for (JournalDescriptor desc: journalIdMap.values()) {
            if (0 == desc.getNumberOfUnconsumedEntries()) {
                removeJournal(desc);
            }
            else if (desc.getFile().isOpen()) {
                try {
                    desc.getFile().forceFlush();
                }
                catch (IOException e) {
                    logger.error("on shutdown - could not fsync journal file, {} -- ignoring", desc.getFile().getFile().getAbsolutePath());
                }
            }
        }
    }

    private String createNewJournalName(String id) {
        return "journal-"+id;
    }

    public JournalDescriptor getCurrentJournalDescriptor() {
        return currentJournalDescriptor;
    }

    public long getFlushPeriodInMs() {
        return flushPeriodInMs;
    }

    public void setFlushPeriodInMs(long flushPeriodInMs) {
        this.flushPeriodInMs = flushPeriodInMs;
    }

    public File getDirectory() {
        return directory;
    }

    public void setDirectory(File directory) {
        this.directory = directory;
    }

    public Map<UUID, JournalDescriptor> getJournalFiles() {
        return journalIdMap;
    }

    public int getNumberOfFlushWorkers() {
        return numberOfFlushWorkers;
    }

    public void setNumberOfFlushWorkers(int numberOfFlushWorkers) {
        this.numberOfFlushWorkers = numberOfFlushWorkers;
    }

    public long getMaxJournalFileSize() {
        return maxJournalFileSize;
    }

    public void setMaxJournalFileSize(long maxJournalFileSize) {
        this.maxJournalFileSize = maxJournalFileSize;
    }

    public long getMaxJournalDurationInMs() {
        return maxJournalDurationInMs;
    }

    public void setMaxJournalDurationInMs(long journalMaxDurationInMs) {
        this.maxJournalDurationInMs = journalMaxDurationInMs;
    }

    public long getJournalsCreated() {
        return journalsCreated.get();
    }

    public long getJournalsRemoved() {
        return journalsRemoved.get();
    }

    public TreeMap<UUID, JournalDescriptor> getJournalIdMap() {
        return journalIdMap;
    }

    public long getNumberOfEntries() {
        return numberOfEntries.get();
    }

    public long getJournalsLoadedAtStartup() {
        return journalsLoadedAtStartup;
    }

    // ------------

    public class JournalReplayIterable implements Iterator<FpqEntry>, Iterable<FpqEntry> {
        private Iterator<JournalDescriptor> jdIter = journalIdMap.values().iterator();
        private Iterator<FpqEntry> entryIter = null;
        private JournalDescriptor jd = null;

        public JournalReplayIterable() throws IOException {
            advanceToNextJournalFile();
        }

        @Override
        public Iterator<FpqEntry> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            // if no descriptor, then we are done
            if (null == entryIter) {
                return false;
            }

            if (!entryIter.hasNext()) {
                try {
                    jd.getFile().close();
                    advanceToNextJournalFile();
                }
                catch (IOException e) {
                    logger.error("exception while closing journal file", e);
                }
            }

            return null != entryIter && entryIter.hasNext();
        }

        @Override
        public FpqEntry next() {
            if (hasNext()) {
                return entryIter.next();
            }
            else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException(JournalMgr.class.getName() + " does not support remove");
        }

        private void advanceToNextJournalFile() throws IOException {
            while (jdIter.hasNext()) {
                jd = jdIter.next();
                if (jd.isWritingFinished()) {
                    entryIter = jd.getFile().iterator();
                    return;
                }
                else {
                    logger.debug("trying to replay a journal that is not 'write finished' : " + jd.getId());
                }
            }
            jd = null;
            entryIter = null;
        }

        public void close() throws IOException {
            if (null != jd) {
                jd.getFile().close();
            }
        }
    }
}
