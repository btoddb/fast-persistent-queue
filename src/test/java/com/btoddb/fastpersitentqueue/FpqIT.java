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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;


/**
 *
 */
public class FpqIT {
    File theDir;
    Fpq fpq1;

    @Test
    public void testPushNoCommit() throws Exception {
        fpq1.init();

        FpqContext ctxt = fpq1.createContext();
        fpq1.push(ctxt, new byte[10]);
        fpq1.push(ctxt, new byte[10]);
        fpq1.push(ctxt, new byte[10]);

        assertThat(ctxt.isPushing(), is(true));
        assertThat(ctxt.isPopping(), is(false));
        assertThat(ctxt.getQueue(), hasSize(3));
        assertThat(fpq1.getMemoryMgr().size(), is(0L));

        fpq1.commit(ctxt);

        assertThat(ctxt.isPushing(), is(false));
        assertThat(ctxt.isPopping(), is(false));
        assertThat(ctxt.getQueue(), is(nullValue()));
        assertThat(fpq1.getMemoryMgr().size(), is(3L));
        assertThat(fpq1.getJournalMgr().getCurrentJournalDescriptor().getNumberOfUnconsumedEntries(), is(3L));
    }

    @Test
    public void testPushExceedTxMax() throws Exception {
        fpq1.setMaxTransactionSize(2);
        fpq1.init();

        FpqContext ctxt = fpq1.createContext();
        fpq1.push(ctxt, new byte[10]);
        fpq1.push(ctxt, new byte[10]);

        try {
            fpq1.push(ctxt, new byte[10]);
            fail("should have thrown exception because of exceeding transaction size");
        }
        catch (FpqException e) {
            // yay!!
        }

    }

    @Test
    public void testPop() throws Exception {
        fpq1.init();

        FpqContext ctxt = fpq1.createContext();
        fpq1.push(ctxt, new byte[10]);
        fpq1.push(ctxt, new byte[10]);
        fpq1.push(ctxt, new byte[10]);
        fpq1.commit(ctxt);

        assertThat(fpq1.getMemoryMgr().size(), is(3L));

        Collection<FpqEntry> entries = fpq1.pop(ctxt, fpq1.getMaxTransactionSize());

        assertThat(entries, hasSize(3));
        assertThat(ctxt.isPushing(), is(false));
        assertThat(ctxt.isPopping(), is(true));
        assertThat(ctxt.getQueue(), hasSize(3));
        assertThat(fpq1.getMemoryMgr().size(), is(0L));
        assertThat(fpq1.getJournalMgr().getCurrentJournalDescriptor().getNumberOfUnconsumedEntries(), is(3L));

        fpq1.commit(ctxt);

        assertThat(ctxt.isPushing(), is(false));
        assertThat(ctxt.isPopping(), is(false));
        assertThat(ctxt.getQueue(), is(nullValue()));
        assertThat(fpq1.getMemoryMgr().size(), is(0L));
        assertThat(fpq1.getJournalMgr().getCurrentJournalDescriptor().getNumberOfUnconsumedEntries(), is(0L));
    }

    @Test
    public void testPushAndPopMultipleTimesOneContext() throws Exception {
        fpq1.init();

        FpqContext context = fpq1.createContext();
        fpq1.push(context, "one".getBytes());
        fpq1.push(context, "two".getBytes());
        fpq1.push(context, "three".getBytes());
        fpq1.commit(context);

        assertThat(fpq1.getNumberOfEntries(), is(3L));

        context = fpq1.createContext();
        assertThat(new String(fpq1.pop(context, 1).iterator().next().getData()), is("one"));
        assertThat(new String(fpq1.pop(context, 1).iterator().next().getData()), is("two"));
        assertThat(new String(fpq1.pop(context, 1).iterator().next().getData()), is("three"));
        fpq1.commit(context);

        assertThat(fpq1.getNumberOfEntries(), is(0L));
        assertThat(fpq1.getNumberOfPushes(), is(3L));
        assertThat(fpq1.getNumberOfPops(), is(3L));
    }

    @Test
    public void testReplay() throws Exception {
        fpq1.setMaxTransactionSize(100);
        fpq1.setMaxMemorySegmentSizeInBytes(1000);
        fpq1.setMaxJournalFileSize(1000);

        int numEntries = 250;
        fpq1.init();

        for (int i=0;i < numEntries;i++) {
            byte[] data = new byte[100];
            ByteBuffer.wrap(data).putInt(i);

            FpqContext ctxt = fpq1.createContext();
            fpq1.push(ctxt, data);
            fpq1.commit(ctxt);
        }

        // simulate improper shutdown by starting another
        Fpq fpq2 = new Fpq();
        fpq2.setQueueName("fpq2");
        fpq2.setMaxTransactionSize(fpq1.getMaxTransactionSize());
        fpq2.setMaxMemorySegmentSizeInBytes(fpq1.getMaxMemorySegmentSizeInBytes());
        fpq2.setMaxJournalFileSize(fpq1.getMaxJournalFileSize());
        fpq2.setMaxJournalDurationInMs(fpq1.getMaxJournalDurationInMs());
        fpq2.setFlushPeriodInMs(fpq1.getFlushPeriodInMs());
        fpq2.setNumberOfFlushWorkers(fpq1.getNumberOfFlushWorkers());
        fpq2.setJournalDirectory(fpq1.getJournalDirectory());
        fpq2.setPagingDirectory(fpq1.getPagingDirectory());
        fpq2.init();

        assertThat(fpq2.getNumberOfEntries(), is(fpq1.getNumberOfEntries()));
    }

    @Test
    public void testMultipleQueues() throws Exception {
        fpq1.init();

        Fpq fpq2 = new Fpq();
        fpq2.setQueueName("fpq2");
        fpq2.setMaxTransactionSize(100);
        fpq2.setMaxMemorySegmentSizeInBytes(10000);
        fpq2.setMaxJournalFileSize(10000);
        fpq2.setMaxJournalDurationInMs(30000);
        fpq2.setFlushPeriodInMs(1000);
        fpq2.setNumberOfFlushWorkers(4);
        fpq2.setJournalDirectory(new File(new File(theDir, "fp2"), "journal"));
        fpq2.setPagingDirectory(new File(new File(theDir, "fp2"), "paging"));
        fpq2.init();

        for (int i=0;i < 1000;i++) {
            FpqContext context1 = fpq1.createContext();
            FpqContext context2 = fpq1.createContext();
            fpq1.push(context1, new byte[100]);
            fpq2.push(context2, new byte[100]);
            fpq1.push(context1, new byte[100]);
            fpq2.push(context2, new byte[100]);
            fpq1.push(context1, new byte[100]);
            fpq2.push(context2, new byte[100]);
            fpq1.commit(context1);
            fpq2.commit(context2);
        }

        for (int i=0;i < 1000;i++) {
            FpqContext context1 = fpq1.createContext();
            FpqContext context2 = fpq1.createContext();
            fpq1.pop(context1, 1);
            fpq2.pop(context2, 1);
            fpq1.pop(context1, 1);
            fpq2.pop(context2, 1);
            fpq1.pop(context1, 1);
            fpq2.pop(context2, 1);
            fpq1.commit(context1);
            fpq2.commit(context2);
        }

        assertThat(fpq1.getNumberOfEntries(), is(0L));
        assertThat(fpq1.getNumberOfPushes(), is(3000L));
        assertThat(fpq1.getNumberOfPops(), is(3000L));
        assertThat(fpq2.getNumberOfEntries(), is(0L));
        assertThat(fpq2.getNumberOfPushes(), is(3000L));
        assertThat(fpq2.getNumberOfPops(), is(3000L));
    }

    @Test
    public void testThreading() throws Exception {
        final int numEntries = 10000;
        final int numPushers = 4;
        final int numPoppers = 4;
        final int entrySize = 1000;
        fpq1.setMaxTransactionSize(2000);
        final int popBatchSize = 100;
        fpq1.setMaxMemorySegmentSizeInBytes(10000000);
        fpq1.setMaxJournalFileSize(10000000);
        fpq1.setMaxJournalDurationInMs(30000);
        fpq1.setFlushPeriodInMs(1000);
        fpq1.setNumberOfFlushWorkers(4);

        final Random pushRand = new Random(1000L);
        final Random popRand = new Random(1000000L);
        final AtomicInteger pusherFinishCount = new AtomicInteger();
        final AtomicInteger numPops = new AtomicInteger();
        final AtomicLong counter = new AtomicLong();
        final AtomicLong pushSum = new AtomicLong();
        final AtomicLong popSum = new AtomicLong();

        fpq1.init();

        ExecutorService execSrvc = Executors.newFixedThreadPool(numPushers + numPoppers);

        Set<Future> futures = new HashSet<Future>();

        // start pushing
        for (int i = 0; i < numPushers; i++) {
            Future future = execSrvc.submit(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < numEntries; i++) {
                        try {
                            long x = counter.getAndIncrement();
                            pushSum.addAndGet(x);
                            ByteBuffer bb = ByteBuffer.wrap(new byte[entrySize]);
                            bb.putLong(x);

                            FpqContext context = fpq1.createContext();
                            fpq1.push(context, bb.array());
                            fpq1.commit(context);
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
        for (int i = 0; i < numPoppers; i++) {
            Future future = execSrvc.submit(new Runnable() {
                @Override
                public void run() {
                    while (pusherFinishCount.get() < numPushers || !fpq1.isEmpty()) {
                        try {
                            FpqContext context = fpq1.createContext();
                            Collection<FpqEntry> entries;
                            while (null != (entries= fpq1.pop(context, popBatchSize))) {
                                for (FpqEntry entry : entries) {
                                    ByteBuffer bb = ByteBuffer.wrap(entry.getData());
                                    popSum.addAndGet(bb.getLong());
                                }
                                numPops.addAndGet(entries.size());
                                fpq1.commit(context);
                                entries.clear();
                                Thread.sleep(popRand.nextInt(10));
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

        assertThat(numPops.get(), is(numEntries * numPushers));
        assertThat(fpq1.getNumberOfEntries(), is(0L));
        assertThat(pushSum.get(), is(popSum.get()));
        assertThat(fpq1.getMemoryMgr().getNumberOfActiveSegments(), is(1));
        assertThat(fpq1.getMemoryMgr().getSegments(), hasSize(1));
        assertThat(fpq1.getJournalMgr().getJournalFiles().entrySet(), hasSize(1));
        assertThat(FileUtils.listFiles(fpq1.getPagingDirectory(), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE), is(empty()));
        assertThat(FileUtils.listFiles(fpq1.getJournalDirectory(), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE), hasSize(1));
    }

    // --------------

    @Before
    public void setup() throws IOException {
        theDir = new File("junitTmp_"+ UUID.randomUUID().toString());
        FileUtils.forceMkdir(theDir);

        fpq1 = new Fpq();
        fpq1.setQueueName("fpq1");
        fpq1.setMaxTransactionSize(100);
        fpq1.setMaxMemorySegmentSizeInBytes(10000);
        fpq1.setMaxJournalFileSize(10000);
        fpq1.setMaxJournalDurationInMs(30000);
        fpq1.setFlushPeriodInMs(1000);
        fpq1.setNumberOfFlushWorkers(4);
        fpq1.setJournalDirectory(new File(new File(theDir, "fp1"), "journal"));
        fpq1.setPagingDirectory(new File(new File(theDir, "fp1"), "paging"));
    }

    @After
    public void cleanup() throws IOException {
        fpq1.shutdown();
        FileUtils.deleteDirectory(theDir);
    }

}
