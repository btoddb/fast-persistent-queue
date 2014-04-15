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
    Fpq fpq;

    @Test
    public void testPushNoCommit() throws Exception {
        fpq.init();

        FpqContext ctxt = fpq.createContext();
        fpq.push(ctxt, new byte[10]);
        fpq.push(ctxt, new byte[10]);
        fpq.push(ctxt, new byte[10]);

        assertThat(ctxt.isPushing(), is(true));
        assertThat(ctxt.isPopping(), is(false));
        assertThat(ctxt.getQueue(), hasSize(3));
        assertThat(fpq.getMemoryMgr().size(), is(0L));

        fpq.commit(ctxt);

        assertThat(ctxt.isPushing(), is(false));
        assertThat(ctxt.isPopping(), is(false));
        assertThat(ctxt.getQueue(), is(nullValue()));
        assertThat(fpq.getMemoryMgr().size(), is(3L));
        assertThat(fpq.getJournalMgr().getCurrentJournalDescriptor().getNumberOfUnconsumedEntries(), is(3L));
    }

    @Test
    public void testPushExceedTxMax() throws Exception {
        fpq.setMaxTransactionSize(2);
        fpq.init();

        FpqContext ctxt = fpq.createContext();
        fpq.push(ctxt, new byte[10]);
        fpq.push(ctxt, new byte[10]);

        try {
            fpq.push(ctxt, new byte[10]);
            fail("should have thrown exception because of exceeding transaction size");
        }
        catch (FpqException e) {
            // yay!!
        }

    }

    @Test
    public void testPop() throws Exception {
        fpq.init();

        FpqContext ctxt = fpq.createContext();
        fpq.push(ctxt, new byte[10]);
        fpq.push(ctxt, new byte[10]);
        fpq.push(ctxt, new byte[10]);
        fpq.commit(ctxt);

        assertThat(fpq.getMemoryMgr().size(), is(3L));

        Collection<FpqEntry> entries = fpq.pop(ctxt, fpq.getMaxTransactionSize());

        assertThat(entries, hasSize(3));
        assertThat(ctxt.isPushing(), is(false));
        assertThat(ctxt.isPopping(), is(true));
        assertThat(ctxt.getQueue(), hasSize(3));
        assertThat(fpq.getMemoryMgr().size(), is(0L));
        assertThat(fpq.getJournalMgr().getCurrentJournalDescriptor().getNumberOfUnconsumedEntries(), is(3L));

        fpq.commit(ctxt);

        assertThat(ctxt.isPushing(), is(false));
        assertThat(ctxt.isPopping(), is(false));
        assertThat(ctxt.getQueue(), is(nullValue()));
        assertThat(fpq.getMemoryMgr().size(), is(0L));
        assertThat(fpq.getJournalMgr().getCurrentJournalDescriptor().getNumberOfUnconsumedEntries(), is(0L));
    }

    @Test
    public void testPushMultipleTimesOneContext() throws Exception {
        fpq.init();
        fail();
    }

    @Test
    public void testPopMultipleTimesOneContext() throws Exception {
        fpq.init();
        fail();
    }

    @Test
    public void testReplay() throws Exception {
        fpq.setMaxTransactionSize(100);
        fpq.setMaxMemorySegmentSizeInBytes(1000);
        fpq.setMaxJournalFileSize(1000);

        int numEntries = 250;
        fpq.init();

        for (int i=0;i < numEntries;i++) {
            byte[] data = new byte[100];
            ByteBuffer.wrap(data).putInt(i);

            FpqContext ctxt = fpq.createContext();
            fpq.push(ctxt, data);
            fpq.commit(ctxt);
        }

        // simulate improper shutdown
        fpq.init();

        fail();
    }

    @Test
    public void testThreading() throws Exception {
        final int numEntries = 10000;
        final int numPushers = 4;
        final int numPoppers = 4;
        final int entrySize = 1000;
        fpq.setMaxTransactionSize(2000);
        final int popBatchSize = 100;
        fpq.setMaxMemorySegmentSizeInBytes(10000000);
        fpq.setMaxJournalFileSize(10000000);
        fpq.setMaxJournalDurationInMs(30000);
        fpq.setFlushPeriodInMs(1000);
        fpq.setNumberOfFlushWorkers(4);

        final Random pushRand = new Random(1000L);
        final Random popRand = new Random(1000000L);
        final AtomicInteger pusherFinishCount = new AtomicInteger();
        final AtomicInteger numPops = new AtomicInteger();
        final AtomicLong counter = new AtomicLong();
        final AtomicLong pushSum = new AtomicLong();
        final AtomicLong popSum = new AtomicLong();

        fpq.init();

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

                            FpqContext context = fpq.createContext();
                            fpq.push(context, bb.array());
                            fpq.commit(context);
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
                    while (pusherFinishCount.get() < numPushers || !fpq.isEmpty()) {
                        try {
                            FpqContext context = fpq.createContext();
                            Collection<FpqEntry> entries;
                            while (null != (entries= fpq.pop(context, popBatchSize))) {
                                for (FpqEntry entry : entries) {
                                    ByteBuffer bb = ByteBuffer.wrap(entry.getData());
                                    popSum.addAndGet(bb.getLong());
                                }
                                numPops.addAndGet(entries.size());
                                fpq.commit(context);
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
        assertThat(fpq.getNumberOfEntries(), is(0L));
        assertThat(pushSum.get(), is(popSum.get()));
        assertThat(fpq.getMemoryMgr().getNumberOfActiveSegments(), is(1));
        assertThat(fpq.getMemoryMgr().getSegments(), hasSize(1));
        assertThat(fpq.getJournalMgr().getJournalFiles().entrySet(), hasSize(1));
        assertThat(FileUtils.listFiles(fpq.getPagingDirectory(), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE), is(empty()));
        assertThat(FileUtils.listFiles(fpq.getJournalDirectory(), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE), hasSize(1));
    }

    // --------------

    @Before
    public void setup() throws IOException {
        theDir = new File("junitTmp_"+ UUID.randomUUID().toString());
        FileUtils.forceMkdir(theDir);

        fpq = new Fpq();
        fpq.setMaxTransactionSize(100);
        fpq.setMaxMemorySegmentSizeInBytes(10000);
        fpq.setMaxJournalFileSize(10000);
        fpq.setMaxJournalDurationInMs(30000);
        fpq.setFlushPeriodInMs(1000);
        fpq.setNumberOfFlushWorkers(4);
        fpq.setJournalDirectory(new File(theDir, "journal"));
        fpq.setPagingDirectory(new File(theDir, "paging"));

        FileUtils.forceMkdir(fpq.getJournalDirectory());
        FileUtils.forceMkdir(fpq.getPagingDirectory());
    }

    @After
    public void cleanup() throws IOException {
        fpq.shutdown();
        FileUtils.deleteDirectory(theDir);
    }

}
