package com.btoddb.fastpersitentqueue;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.fail;


/**
 *
 */
public class JournalMgrIT {
    File theDir;
    JournalMgr mgr;

    @Test
    public void testInitFreshJournalMgr() throws Exception {
        mgr.init();

        assertThat(mgr.getJournalFiles().size(), is(1));
        assertThat(mgr.getJournalIdMap().entrySet(), hasSize(1));

        JournalDescriptor jd = mgr.getJournalFiles().values().iterator().next();
        assertThat(jd.getFile().getFile().getParent(), is(theDir.getAbsolutePath()));
        assertThat(jd.getFile().getFile().exists(), is(true));
        assertThat(jd, sameInstance(mgr.getCurrentJournalDescriptor()));
    }

    @Test
    public void testJournalDirectorySetter() {
        mgr.setDirectory(new File("/dirname"));
        assertThat(mgr.getDirectory(), is(new File("/dirname")));
    }

    @Test
    public void testNumberOfFlushWorkersSetter() {
        mgr.setNumberOfFlushWorkers(4);
        assertThat(mgr.getNumberOfFlushWorkers(), is(4));
    }

    @Test
    public void testFlushPeriodInMsSetter() {
        mgr.setFlushPeriodInMs(1234);
        assertThat(mgr.getFlushPeriodInMs(), is(1234L));
    }

    @Test
    public void testJournalMaxFileSizeSetter() {
        mgr.setMaxJournalFileSize(2345);
        assertThat(mgr.getMaxJournalFileSize(), is(2345L));
    }

    @Test
    public void testJournalRollingBecauseOfSize() throws IOException {
        mgr.setMaxJournalFileSize(100);
        mgr.init();

        JournalDescriptor jd1 = mgr.getCurrentJournalDescriptor();
        mgr.append(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        mgr.append(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        mgr.append(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        mgr.append(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

        // not rolled at this point
        assertThat(jd1, sameInstance(mgr.getCurrentJournalDescriptor()));

        mgr.append(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        mgr.append(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        mgr.append(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        mgr.append(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

        // this should roll the journal file
        mgr.append(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        assertThat(jd1, not(sameInstance(mgr.getCurrentJournalDescriptor())));
        assertThat(jd1.isWritingFinished(), is(true));
        assertThat(mgr.getCurrentJournalDescriptor().isWritingFinished(), is(false));
        assertThat(mgr.getJournalFiles().entrySet(), hasSize(2));
        assertThat(mgr.getJournalIdMap().entrySet(), hasSize(2));
    }

    @Test
    public void testJournalRollingBecauseOfTime() throws Exception {
        mgr.setMaxJournalFileSize(100);
        mgr.setMaxJournalDurationInMs(500);
        mgr.init();

        JournalDescriptor jd1 = mgr.getCurrentJournalDescriptor();
        mgr.append(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

        Thread.sleep(600);

        mgr.append(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        assertThat(jd1, not(sameInstance(mgr.getCurrentJournalDescriptor())));
        assertThat(jd1.isWritingFinished(), is(true));
        assertThat(mgr.getCurrentJournalDescriptor().isWritingFinished(), is(false));
        assertThat(mgr.getJournalFiles().entrySet(), hasSize(2));
        assertThat(mgr.getJournalIdMap().entrySet(), hasSize(2));
    }

    @Test
    public void testAppend() throws IOException {
        byte[] data = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        long now = System.currentTimeMillis();

        mgr.setMaxJournalFileSize(48);
        mgr.init();
        assertThat(mgr.getCurrentJournalDescriptor().getStartTime(), is(0L));

        mgr.append(data);
        assertThat(mgr.getCurrentJournalDescriptor().getStartTime(), is(greaterThanOrEqualTo(now)));
        mgr.append(data);
        assertThat(mgr.getCurrentJournalDescriptor().getStartTime(), is(0L));
        mgr.append(data);

        assertThat(mgr.getCurrentJournalDescriptor().getStartTime(), is(greaterThanOrEqualTo(now)));
        assertThat(mgr.getJournalIdMap().entrySet(), hasSize(2));
    }

    @Test
    public void testReportConsumption() throws IOException {
        mgr.setMaxJournalFileSize(43);
        mgr.init();

        FpqEntry entry1 = mgr.append(new byte[] {0, 1, 2, 3});
        FpqEntry entry2 = mgr.append(new byte[] {0, 1, 2, 3});
        FpqEntry entry3 = mgr.append(new byte[] {0, 1, 2, 3});
        assertThat(mgr.getJournalIdMap().entrySet(), hasSize(2));
        assertThat(mgr.getJournalIdMap().get(entry1.getJournalId()).getNumberOfUnconsumedEntries(), is(2L));
        assertThat(mgr.getJournalIdMap().get(entry3.getJournalId()).getNumberOfUnconsumedEntries(), is(1L));

        mgr.reportTake(entry1);
        assertThat(mgr.getJournalIdMap().get(entry1.getJournalId()).getNumberOfUnconsumedEntries(), is(1L));
        assertThat(mgr.getJournalIdMap().get(entry3.getJournalId()).getNumberOfUnconsumedEntries(), is(1L));

        mgr.reportTake(entry2);
        assertThat(mgr.getJournalIdMap(), not(hasKey(entry1.getJournalId())));
        assertThat(mgr.getJournalIdMap(), not(hasKey(entry2.getJournalId())));
        assertThat(mgr.getJournalIdMap().get(entry3.getJournalId()).getNumberOfUnconsumedEntries(), is(1L));

        mgr.reportTake(entry3);
        assertThat(mgr.getJournalIdMap(), not(hasKey(entry1.getJournalId())));
        assertThat(mgr.getJournalIdMap(), not(hasKey(entry2.getJournalId())));
        assertThat(mgr.getJournalIdMap().get(entry3.getJournalId()).getNumberOfUnconsumedEntries(), is(0L));
    }

    @Test
    public void testAnyWritesHappened() throws IOException {
        mgr.init();
        assertThat(mgr.getCurrentJournalDescriptor().isAnyWritesHappened(), is(false));
        mgr.append(new byte[] {0, 1});
        assertThat(mgr.getCurrentJournalDescriptor().isAnyWritesHappened(), is(true));
    }

    @Test
    public void testShutdownNoRemainingData() throws IOException {
        mgr.setMaxJournalFileSize(35);
        mgr.init();

        FpqEntry entry1 = mgr.append(new byte[] {0, 1});
        FpqEntry entry2 = mgr.append(new byte[] {0, 1});
        FpqEntry entry3 = mgr.append(new byte[] {0, 1});
        assertThat(mgr.getJournalIdMap().entrySet(), hasSize(2));

        mgr.reportTake(entry1);
        mgr.reportTake(entry2);
        mgr.reportTake(entry3);
        mgr.shutdown();

        assertThat(FileUtils.listFiles(theDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE), is(empty()));
        assertThat(mgr.getJournalIdMap(), not(hasKey(entry1.getJournalId())));
        assertThat(mgr.getJournalIdMap(), not(hasKey(entry2.getJournalId())));
        assertThat(mgr.getJournalIdMap(), not(hasKey(entry3.getJournalId())));
    }

    @Test
    public void testShutdownHasRemainingData() throws IOException {
        mgr.setMaxJournalFileSize(43);
        mgr.init();

        FpqEntry entry1 = mgr.append(new byte[] {0, 1, 2, 3});
        FpqEntry entry2 = mgr.append(new byte[] {0, 1, 2, 3});
        FpqEntry entry3 = mgr.append(new byte[] {0, 1, 2, 3});
        assertThat(mgr.getJournalIdMap().entrySet(), hasSize(2));

        mgr.reportTake(entry1);
        mgr.reportTake(entry2);
        mgr.shutdown();

        Collection<File> remainingFiles = FileUtils.listFiles(theDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
        assertThat(remainingFiles, hasSize(1));
        assertThat(remainingFiles, contains(mgr.getJournalIdMap().get(entry3.getJournalId()).getFile().getFile().getAbsoluteFile()));
        assertThat(mgr.getJournalIdMap(), not(hasKey(entry1.getJournalId())));
        assertThat(mgr.getJournalIdMap(), not(hasKey(entry2.getJournalId())));
        assertThat(mgr.getJournalIdMap(), hasKey(entry3.getJournalId()));
    }

    @Test
    public void testLoadingJournalsAtStartup() {
        fail();
    }

    @Test
    public void testJournalEntryIterator() throws IOException {
        int numEntries = 250;
        mgr.setMaxJournalFileSize(100);
        mgr.init();
        for (int i=0;i < numEntries;i++) {
            mgr.append(new byte[] {(byte)i, 1, 2});
        }
        mgr.shutdown();

        mgr.init();
        byte count = 0;
        JournalMgr.JournalReplayIterable replay = mgr.createReplayIterable();
        for (FpqEntry entry : replay) {
            assertThat(entry.getData()[0], is(count++));
        }
        replay.close();
    }

    @Test
    public void testThreading() throws IOException, ExecutionException {
        final int numEntries = 10000;
        final int numPushers = 3;
        int numPoppers = 3;

        final Random pushRand = new Random(1000L);
        final Random popRand = new Random(1000000L);
        final ConcurrentLinkedQueue<FpqEntry> events = new ConcurrentLinkedQueue<FpqEntry>();
        final AtomicInteger pusherFinishCount = new AtomicInteger();
        final AtomicInteger numPops = new AtomicInteger();

        mgr.setMaxJournalFileSize(1000);
        mgr.init();

        ExecutorService execSrvc = Executors.newFixedThreadPool(numPushers+numPoppers);

        Set<Future> futures = new HashSet<Future>();

        // start pushing
        for (int i=0;i < numPushers;i++) {
            Future future = execSrvc.submit(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < numEntries; i++) {
                        try {
                            FpqEntry entry = mgr.append(new byte[100]);
                            events.offer(entry);
                            Thread.sleep(pushRand.nextInt(5));
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    pusherFinishCount.incrementAndGet();
                }
            });
            futures.add(future);
        }

        // start popping
        for (int i=0;i < numPoppers;i++) {
            Future future = execSrvc.submit(new Runnable() {
                @Override
                public void run() {
                    while (pusherFinishCount.get() < numPushers || !events.isEmpty()) {
                        try {
                            FpqEntry entry;
                            while (null != (entry = events.poll())) {
                                numPops.incrementAndGet();
                                mgr.reportTake(entry);
                                Thread.sleep(popRand.nextInt(5));
                            }
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
            futures.add(future);
        }

        boolean finished = false;
        while (!finished) {
            try {
                for (Future f : futures) {
                    f.get();
                }
                finished = true;
            }
            catch (InterruptedException e) {
                // ignore
                Thread.interrupted();
            }
        }

        assertThat(numPops.get(), is(numEntries*numPushers));
        assertThat(mgr.getJournalIdMap().entrySet(), hasSize(1));
        assertThat(FileUtils.listFiles(theDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE), hasSize(1));
    }

    // --------------

    @Before
    public void setup() throws IOException {
        theDir = new File("junitTmp_"+ UUID.randomUUID().toString()).getCanonicalFile();
        FileUtils.forceMkdir(theDir);

        mgr = new JournalMgr();
        mgr.setDirectory(theDir);
        mgr.setNumberOfFlushWorkers(4);
        mgr.setFlushPeriodInMs(1000);
    }

    @After
    public void cleanup() throws IOException {
        mgr.shutdown();
        FileUtils.deleteDirectory(theDir);
    }
}
