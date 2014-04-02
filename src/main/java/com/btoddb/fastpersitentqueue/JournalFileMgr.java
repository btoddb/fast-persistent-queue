package com.btoddb.fastpersitentqueue;

import com.eaio.uuid.UUID;
import com.sun.istack.internal.NotNull;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;


/**
 *
 */
public class JournalFileMgr {
    private static final Logger logger = LoggerFactory.getLogger(JournalFileMgr.class);

    private String journalDirName;
    private long flushPeriodInMs = 10000;
    private int numberOfFlushWorkers = 4;
    private int numberOfGeneralWorkers = 2;
    private long maxJournalFileSize = 100000000L;
    private long maxJournalDurationInMs = (10 * 60 * 1000); // 10 minutes

    private File journalDirFile;

    private volatile JournalDescriptor currentJournalDescriptor;

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
    public void start() throws IOException {
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
        journalDirFile = new File(journalDirName);
        FileUtils.forceMkdir(journalDirFile);
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
        JournalFile jf = new JournalFile(new File(journalDirFile, fn));
        // this could possibly create a race, if the flushPeriodInMs is very very low ;)
        ScheduledFuture future = flushExec.scheduleWithFixedDelay(new FlushRunner(jf), flushPeriodInMs, flushPeriodInMs, TimeUnit.MILLISECONDS);
        JournalDescriptor jd = new JournalDescriptor(id, jf, future);
        journalIdMap.put(id, jd);
        return jd;
    }


    public Entry append(byte[] data) throws IOException {
        JournalDescriptor desc = getCurrentJournalDescriptor();

        Entry entry = desc.getFile().append(new Entry(data));
        entry.setJournalId(desc.getId());

        if (0 >= desc.getStartTime()) {
            desc.setStartTime(System.currentTimeMillis());
        }
        desc.incrementEntryCount();

        rollJournalIfNeeded();

        return entry;
    }

    private void rollJournalIfNeeded() throws IOException {
        boolean rollJournal = false;
        if ( !currentJournalDescriptor.isAnyWritesHappened()) {
            return;
        }

        long fileLength = currentJournalDescriptor.getFile().getRandomAccessWriterFile().length();
        if (fileLength >= maxJournalFileSize ||
                (0 < fileLength && (currentJournalDescriptor.getStartTime()+maxJournalDurationInMs) < System.currentTimeMillis())) {
            rollJournal = currentJournalDescriptor.isRollFileNeeded();
        }

        if (rollJournal) {
            currentJournalDescriptor.setWritingFinished(true);
            currentJournalDescriptor = createAndAddNewJournal();
        }

    }

    public void reportTake(Entry entry) throws IOException {
        JournalDescriptor desc = journalIdMap.get(entry.getJournalId());
        if (null == desc) {
            logger.error("illegal state - reported consumption of journal entry, but journal descriptor no longer exists!");
            return;
        }

        int count = desc.decrementEntryCount();

        synchronized (desc) {
            if (desc.getLastPositionRead() < entry.getFilePosition()) {
                desc.setLastPositionRead(entry.getFilePosition()+entry.getData().length+JournalFile.VERSION_1_OVERHEAD-1);
            }
            if (desc.isWritingFinished() && 0 == count && desc.getLength() == desc.getLastPositionRead()-1) {
                scheduleJournalRemoval(desc);
            }
        }
    }

    private void scheduleJournalRemoval(final JournalDescriptor desc) {
        generalExec.submit(new Runnable() {
            @Override
            public void run() {
                desc.getFuture().cancel(false);

            }
        });
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

    public String getJournalDirName() {
        return journalDirName;
    }

    public void setJournalDirName(String journalDirName) {
        this.journalDirName = journalDirName;
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
}
