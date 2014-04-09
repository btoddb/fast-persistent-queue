package com.btoddb.fastpersitentqueue;

import com.eaio.uuid.UUID;
import org.apache.commons.io.FileUtils;
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
    private AtomicLong journalsCreated = new AtomicLong();
    private AtomicLong journalsRemoved = new AtomicLong();

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
        reloadJournalFiles();
        if (journalIdMap.isEmpty()) {
            currentJournalDescriptor = createAndAddNewJournal();
        }
        else {
            currentJournalDescriptor = journalIdMap.lastEntry().getValue();
        }
    }

    private void prepareJournaling() throws IOException {
        FileUtils.forceMkdir(directory);
    }

    private void reloadJournalFiles() {
        // nothing yet
        // - load and sorted
    }

    private JournalDescriptor createAndAddNewJournal() throws IOException {
        UUID uid = new UUID();
        String fn = createNewJournalName(uid.toString());
        return addNewJournal(uid, fn);
        }

    private JournalDescriptor addNewJournal(UUID id, String fn) throws IOException {
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
        return jd;
    }


    public FpqEntry append(byte[] event) throws IOException {
        Collection<FpqEntry> entries = append(Collections.singleton(event));
        return entries.iterator().next();
    }

    public Collection<FpqEntry> append(Collection<byte[]> events) throws IOException {
        if (shutdownInProgress) {
            throw new FpqException("FPQ has been shutdown or is in progress, cannot append");
        }

        // TODO:BTB - optimize this by calling file.append() with collection of data
        List<FpqEntry> entryList = new ArrayList<FpqEntry>(events.size());
        for (byte[] data : events) {
            // TODO:BTB - could use multiple journal files to prevent lock contention - each thread in an "append" pool gets one?
            boolean appended = false;
            while (!appended) {
                JournalDescriptor desc = getCurrentJournalDescriptor();
                if (null == desc) {
                    Utils.logAndThrow(logger, String.format("no current journal descriptor.  did you call %s.init()?", this.getClass().getSimpleName()));
                }
                synchronized (desc) {
                    if (!desc.isWritingFinished()) {
                        FpqEntry entry = desc.getFile().append(new FpqEntry(data));
                        entry.setJournalId(desc.getId());
                        entryList.add(entry);

                        if (0 >= desc.getStartTime()) {
                            desc.setStartTime(System.currentTimeMillis());
                        }
                        desc.incrementEntryCount(1);

                        rollJournalIfNeeded();
                        appended = true;
                    }
                }
            }
        }

        return entryList;
    }

    // this method must be synchronized from above
    private void rollJournalIfNeeded() throws IOException {
        if ( !currentJournalDescriptor.isAnyWritesHappened()) {
            return;
        }

        long fileLength = currentJournalDescriptor.getFile().getWriterFilePosition();
        if (fileLength >= maxJournalFileSize ||
                (0 < fileLength && (currentJournalDescriptor.getStartTime()+maxJournalDurationInMs) < System.currentTimeMillis())) {
            currentJournalDescriptor.setWritingFinished(true);
            currentJournalDescriptor.getFuture().cancel(false);
            currentJournalDescriptor.getFile().forceFlush();
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

        for (FpqEntry entry : entries) {
            JournalDescriptor desc;
            journalLock.readLock().lock();
            try {
                desc = journalIdMap.get(entry.getJournalId());
            }
            finally {
                journalLock.readLock().unlock();
            }

            if (null == desc) {
                logger.error("illegal state - reported consumption of journal entry, but journal descriptor doesn't exist!");
                continue;
            }

            long remaining = desc.decrementEntryCount(1);

            // this is therad-safe, because only one thread can decrement down to zero when isWritingFinished
            if (0 == remaining && desc.isWritingFinished()) {
                submitJournalRemoval(desc);
            }
        }
    }

    private void submitJournalRemoval(final JournalDescriptor desc) {
        generalExec.submit(new Runnable() {
            @Override
            public void run() {
                removeJournal(desc);
            }
        });
    }

    private void removeJournal(JournalDescriptor desc) {
        desc.getFuture().cancel(false);
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

        journalsRemoved.incrementAndGet();
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

        for (JournalDescriptor desc: journalIdMap.values()) {
            if (0 == desc.getNumberOfUnconsumedEntries()) {
                removeJournal(desc);
            }
            else {
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
}
