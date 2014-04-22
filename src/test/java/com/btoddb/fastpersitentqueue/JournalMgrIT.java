package com.btoddb.fastpersitentqueue;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;


/**
 *
 */
public class JournalMgrIT {
    File theDir;
    JournalMgr mgr;
    AtomicLong idGen = new AtomicLong();

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
        mgr.setMaxJournalFileSize(150);
        mgr.init();

        JournalDescriptor jd1 = mgr.getCurrentJournalDescriptor();
        mgr.append(new FpqEntry(idGen.incrementAndGet(), new byte[9]));
        mgr.append(new FpqEntry(idGen.incrementAndGet(), new byte[9]));
        mgr.append(new FpqEntry(idGen.incrementAndGet(), new byte[9]));
        mgr.append(new FpqEntry(idGen.incrementAndGet(), new byte[9]));

        // not rolled at this point
        assertThat(jd1, sameInstance(mgr.getCurrentJournalDescriptor()));

        mgr.append(new FpqEntry(idGen.incrementAndGet(), new byte[9]));
        mgr.append(new FpqEntry(idGen.incrementAndGet(), new byte[9]));
        mgr.append(new FpqEntry(idGen.incrementAndGet(), new byte[9]));
        mgr.append(new FpqEntry(idGen.incrementAndGet(), new byte[9]));

        // this should roll the journal file
        mgr.append(new FpqEntry(idGen.incrementAndGet(), new byte[9]));
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
        mgr.append(new FpqEntry(idGen.incrementAndGet(), new byte[9]));

        Thread.sleep(600);

        mgr.append(new FpqEntry(idGen.incrementAndGet(), new byte[9]));
        assertThat(jd1, not(sameInstance(mgr.getCurrentJournalDescriptor())));
        assertThat(jd1.isWritingFinished(), is(true));
        assertThat(mgr.getCurrentJournalDescriptor().isWritingFinished(), is(false));
        assertThat(mgr.getJournalFiles().entrySet(), hasSize(2));
        assertThat(mgr.getJournalIdMap().entrySet(), hasSize(2));
    }

    @Test
    public void testAppend() throws IOException {
        byte[] data = new byte[9];
        long now = System.currentTimeMillis();

        mgr.setMaxJournalFileSize(50);
        mgr.init();
        assertThat(mgr.getCurrentJournalDescriptor().getStartTime(), is(0L));

        mgr.append(new FpqEntry(idGen.incrementAndGet(), data));
        assertThat(mgr.getCurrentJournalDescriptor().getStartTime(), is(greaterThanOrEqualTo(now)));
        mgr.append(new FpqEntry(idGen.incrementAndGet(), data));
        assertThat(mgr.getCurrentJournalDescriptor().getStartTime(), is(0L));
        mgr.append(new FpqEntry(idGen.incrementAndGet(), data));

        assertThat(mgr.getCurrentJournalDescriptor().getStartTime(), is(greaterThanOrEqualTo(now)));
        assertThat(mgr.getJournalIdMap().entrySet(), hasSize(2));
    }

    @Test
    public void testReportConsumption() throws IOException {
        mgr.setMaxJournalFileSize(51);
        mgr.init();

        FpqEntry entry1 = mgr.append(new FpqEntry(idGen.incrementAndGet(), new byte[] {0, 1, 2, 3}));
        FpqEntry entry2 = mgr.append(new FpqEntry(idGen.incrementAndGet(), new byte[] {0, 1, 2, 3}));
        FpqEntry entry3 = mgr.append(new FpqEntry(idGen.incrementAndGet(), new byte[] {0, 1, 2, 3}));
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
        mgr.append(new FpqEntry(idGen.incrementAndGet(), new byte[] {0, 1}));
        assertThat(mgr.getCurrentJournalDescriptor().isAnyWritesHappened(), is(true));
    }

    @Test
    public void testShutdownNoRemainingData() throws IOException {
        mgr.setMaxJournalFileSize(43);
        mgr.init();

        FpqEntry entry1 = mgr.append(new FpqEntry(idGen.incrementAndGet(), new byte[] {0, 1}));
        FpqEntry entry2 = mgr.append(new FpqEntry(idGen.incrementAndGet(), new byte[] {0, 1}));
        FpqEntry entry3 = mgr.append(new FpqEntry(idGen.incrementAndGet(), new byte[] {0, 1}));
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
        mgr.setMaxJournalFileSize(51);
        mgr.init();

        FpqEntry entry1 = mgr.append(new FpqEntry(idGen.incrementAndGet(), new byte[] {0, 1, 2, 3}));
        FpqEntry entry2 = mgr.append(new FpqEntry(idGen.incrementAndGet(), new byte[] {0, 1, 2, 3}));
        FpqEntry entry3 = mgr.append(new FpqEntry(idGen.incrementAndGet(), new byte[] {0, 1, 2, 3}));
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
    public void testLoadEntriesOnStartupUsingIterator() throws Exception {
        mgr.setMaxJournalFileSize(60);
        mgr.init();

        for (int i=0;i < 100;i++) {
            mgr.append(new FpqEntry(idGen.incrementAndGet(), ByteBuffer.allocate(10).putInt(i).array()));
        }

        assertThat(mgr.getJournalIdMap().entrySet(), hasSize(51));
        mgr.shutdown();

        JournalMgr mgr2 = new JournalMgr();
        mgr2.setMaxJournalDurationInMs(mgr.getMaxJournalDurationInMs());
        mgr2.setFlushPeriodInMs(mgr.getFlushPeriodInMs());
        mgr2.setDirectory(mgr.getDirectory());
        mgr2.setMaxJournalFileSize(mgr.getMaxJournalFileSize());
        mgr2.setNumberOfFlushWorkers(mgr.getNumberOfFlushWorkers());
        mgr2.init();

        assertThat(mgr2.getNumberOfEntries(), is(100L));

        int count = 0;
        JournalMgr.JournalReplayIterable iter = mgr2.createReplayIterable();
        try {
            while (iter.hasNext()) {
                assertThat(ByteBuffer.wrap(iter.next().getData()).getInt(), is(count));
                count++;
            }
            assertThat(count, is(100));
        }
        finally {
            iter.close();
            mgr.shutdown();
        }
    }

    @Test
    public void testAnotherIteratorTest() throws Exception {
        mgr.setMaxJournalFileSize(1000);
        mgr.init();

        int numEntries = 250;

        for (int i=0;i < numEntries;i++) {
            byte[] data = new byte[100];
            ByteBuffer.wrap(data).putInt(i);
            mgr.append(new FpqEntry(i, data));
        }
//        mgr.shutdown();

        JournalMgr mgr2 = new JournalMgr();
        mgr2.setNumberOfFlushWorkers(mgr.getNumberOfFlushWorkers());
        mgr2.setMaxJournalFileSize(mgr.getMaxJournalFileSize());
        mgr2.setDirectory(mgr.getDirectory());
        mgr2.setFlushPeriodInMs(mgr.getFlushPeriodInMs());
        mgr2.setMaxJournalDurationInMs(mgr.getMaxJournalDurationInMs());
        mgr2.init();

        JournalMgr.JournalReplayIterable replayer = mgr2.createReplayIterable();
        int count = 0;
        for (FpqEntry entry : replayer) {
            System.out.println("count = " + count);
            assertThat(ByteBuffer.wrap(entry.getData()).getInt(), is(count));
            count++;
        }

        assertThat((long)count, is(mgr.getNumberOfEntries()));
        assertThat(mgr2.getNumberOfEntries(), is(mgr.getNumberOfEntries()));

    }
    @Test
    public void testLoadEntriesOnExistingManagerUsingIterator() throws Exception {
        mgr.setMaxJournalFileSize(60);
        mgr.init();

        for (int i=0;i < 100;i++) {
            mgr.append(new FpqEntry(idGen.incrementAndGet(), ByteBuffer.allocate(10).putInt(i).array()));
        }

        assertThat(mgr.getJournalIdMap().entrySet(), hasSize(51));

        int count = 0;
        JournalMgr.JournalReplayIterable iter = mgr.createReplayIterable();
        try {
            while (iter.hasNext()) {
                assertThat(ByteBuffer.wrap(iter.next().getData()).getInt(), is(count));
                count++;
            }
            assertThat(count, is(100));
        }
        finally {
            iter.close();
        }
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
                            FpqEntry entry = mgr.append(new FpqEntry(idGen.incrementAndGet(), new byte[100]));
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
