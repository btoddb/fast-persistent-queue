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
    public void testPushNoTx() throws Exception {
        fpq1.init();

        try {
            fpq1.push(new byte[10]);
            fail("should have thrown FpqException because no transaction");
        }
        catch (FpqException e) {
            assertThat(e.getMessage(), containsString("transaction not started"));
        }
    }

    @Test
    public void testPopNoTx() throws Exception {
        fpq1.init();
        fpq1.beginTransaction();
        fpq1.push(new byte[10]);
        fpq1.commit();

        try {
            fpq1.pop(1);
            fail("should have thrown FpqException because no transaction");
        }
        catch (FpqException e) {
            assertThat(e.getMessage(), containsString("transaction not started"));
        }
    }

    @Test
    public void testPushNoCommit() throws Exception {
        fpq1.init();

        fpq1.beginTransaction();
        fpq1.push(new byte[10]);
        fpq1.push(new byte[10]);
        fpq1.push(new byte[10]);

        assertThat(fpq1.getContext().isPushing(), is(true));
        assertThat(fpq1.getContext().isPopping(), is(false));
        assertThat(fpq1.getContext().getQueue(), hasSize(3));
        assertThat(fpq1.getMemoryMgr().size(), is(0L));

        fpq1.commit();

        assertThat(fpq1.getMemoryMgr().size(), is(3L));
        assertThat(fpq1.getJournalMgr().getCurrentJournalDescriptor().getNumberOfUnconsumedEntries(), is(3L));
    }

    @Test
    public void testPushExceedTxMax() throws Exception {
        fpq1.setMaxTransactionSize(2);
        fpq1.init();

        fpq1.beginTransaction();
        fpq1.push(new byte[10]);
        fpq1.push(new byte[10]);

        try {
            fpq1.push(new byte[10]);
            fail("should have thrown exception because of exceeding transaction size");
        }
        catch (FpqException e) {
            // yay!!
        }
        finally {
            fpq1.rollback();
        }
    }

    @Test
    public void testPop() throws Exception {
        fpq1.init();

        fpq1.beginTransaction();
        fpq1.push(new byte[10]);
        fpq1.push(new byte[10]);
        fpq1.push(new byte[10]);
        fpq1.commit();

        assertThat(fpq1.getMemoryMgr().size(), is(3L));

        fpq1.beginTransaction();
        Collection<FpqEntry> entries = fpq1.pop(fpq1.getMaxTransactionSize());

        assertThat(entries, hasSize(3));
        assertThat(fpq1.getContext().isPushing(), is(false));
        assertThat(fpq1.getContext().isPopping(), is(true));
        assertThat(fpq1.getContext().getQueue(), hasSize(3));
        assertThat(fpq1.getMemoryMgr().size(), is(0L));
        assertThat(fpq1.getJournalMgr().getCurrentJournalDescriptor().getNumberOfUnconsumedEntries(), is(3L));

        fpq1.commit();

        assertThat(fpq1.getMemoryMgr().size(), is(0L));
        assertThat(fpq1.getJournalMgr().getCurrentJournalDescriptor().getNumberOfUnconsumedEntries(), is(0L));
    }

    @Test
    public void testPushAndPopMultipleTimesOneContext() throws Exception {
        fpq1.init();

        fpq1.beginTransaction();
        fpq1.push("one".getBytes());
        fpq1.push("two".getBytes());
        fpq1.push("three".getBytes());
        fpq1.commit();

        assertThat(fpq1.getNumberOfEntries(), is(3L));

        fpq1.beginTransaction();
        assertThat(new String(fpq1.pop(1).iterator().next().getData()), is("one"));
        assertThat(new String(fpq1.pop(1).iterator().next().getData()), is("two"));
        assertThat(new String(fpq1.pop(1).iterator().next().getData()), is("three"));
        fpq1.commit();

        assertThat(fpq1.getNumberOfEntries(), is(0L));
        assertThat(fpq1.getJmxMetrics().getPushCount(), is(3L));
        assertThat(fpq1.getJmxMetrics().getPopCount(), is(3L));
    }

    @Test
    public void testRollbackPushes() throws Exception {
        fpq1.init();

        int numEntries = 250;
        int sum = pushEntries(numEntries, fpq1, 100);

        fpq1.beginTransaction();
        fpq1.push(new byte[10]);
        fpq1.push(new byte[10]);
        fpq1.push(new byte[10]);
        fpq1.rollback();

        checkSumByPopping(fpq1, numEntries, sum);
    }

    @Test
    public void testRollbackPops() throws Exception {
        fpq1.init();

        int numEntries = 250;
        int sum = pushEntries(numEntries, fpq1, 100);

        fpq1.beginTransaction();
        fpq1.pop(10);
        fpq1.pop(1);
        fpq1.pop(5);
        fpq1.rollback();

        checkSumByPopping(fpq1, numEntries, sum);
    }

    @Test
    public void testReplayAfterShutdown() throws Exception {
        fpq1.setMaxTransactionSize(100);
        fpq1.setMaxMemorySegmentSizeInBytes(1000);
        fpq1.setMaxJournalFileSize(1000);
        fpq1.init();

        int numEntries = 250;
        int sum = pushEntries(numEntries, fpq1, 100);

        assertThat(fpq1.getMemoryMgr().getNumberOfEntries(), is((long) numEntries));
        assertThat(fpq1.getJournalMgr().getNumberOfEntries(), is((long) numEntries));
        fpq1.shutdown();

        checkSumByPopping(numEntries, sum);
    }

    @Test
    public void testReplayWithoutShutdown() throws Exception {
        fpq1.setMaxTransactionSize(100);
        fpq1.setMaxMemorySegmentSizeInBytes(1000);
        fpq1.setMaxJournalFileSize(1000);
        fpq1.init();

        int numEntries = 250;
        int sum = pushEntries(numEntries, fpq1, 100);

        // no shutdown, just fire-up another FPQ

        checkSumByPopping(numEntries, sum);
    }

    @Test
    public void testReplayAfterPopTxWithoutShutdown() throws Exception {
        fpq1.setMaxTransactionSize(100);
        fpq1.setMaxMemorySegmentSizeInBytes(1000);
        fpq1.setMaxJournalFileSize(1000);
        fpq1.init();

        int numEntries = 250;
        int sum = pushEntries(numEntries, fpq1, 100);

        fpq1.beginTransaction();
        fpq1.pop(1);
        fpq1.pop(5);
        fpq1.pop(4);

        // no shutdown, just fire-up another FPQ to see if replay works
        // even tho some have been popped and not committed

        checkSumByPopping(numEntries, sum);

        // cleanup - makes shutdown faster
        fpq1.rollback();
    }

    @Test
    public void testReplayAfterPushTxWithoutShutdown() throws Exception {
        fpq1.setMaxTransactionSize(100);
        fpq1.setMaxMemorySegmentSizeInBytes(1000);
        fpq1.setMaxJournalFileSize(1000);
        fpq1.init();

        int numEntries = 250;
        int sum = pushEntries(numEntries, fpq1, 100);

        fpq1.beginTransaction();
        fpq1.push(new byte[10]);
        fpq1.push(new byte[10]);
        fpq1.push(new byte[10]);

        // no shutdown, just fire-up another FPQ

        checkSumByPopping(numEntries, sum);

        // cleanup - makes shutdown faster
        fpq1.rollback();
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
            fpq1.beginTransaction();
            fpq2.beginTransaction();
            fpq1.push(new byte[100]);
            fpq2.push(new byte[100]);
            fpq1.push(new byte[100]);
            fpq2.push(new byte[100]);
            fpq1.push(new byte[100]);
            fpq2.push(new byte[100]);
            fpq1.commit();
            fpq2.commit();
        }

        long end = System.currentTimeMillis()+10000;
        while ((0 < fpq1.getNumberOfEntries() || 0 < fpq2.getNumberOfEntries())
                && System.currentTimeMillis() < end) {
            fpq1.beginTransaction();
            fpq2.beginTransaction();
            fpq1.pop(1);
            fpq2.pop(1);
            fpq1.pop(1);
            fpq2.pop(1);
            fpq1.pop(1);
            fpq2.pop(1);
            fpq1.commit();
            fpq2.commit();
        }

        assertThat(fpq1.getNumberOfEntries(), is(0L));
        assertThat(fpq1.getJmxMetrics().getPushCount(), is(3000L));
        assertThat(fpq1.getJmxMetrics().getPopCount(), is(3000L));
        assertThat(fpq2.getNumberOfEntries(), is(0L));
        assertThat(fpq2.getJmxMetrics().getPushCount(), is(3000L));
        assertThat(fpq2.getJmxMetrics().getPopCount(), is(3000L));
    }

    @Test
    public void testThreading() throws Exception {
        final int numEntries = 1000;
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

                            fpq1.beginTransaction();
                            fpq1.push(bb.array());
                            fpq1.commit();
                            if ((x+1) % 500 == 0) {
                                System.out.println("pushed ID = " + x);
                            }
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
                            fpq1.beginTransaction();
                            try {
                                Collection<FpqEntry> entries = fpq1.pop(popBatchSize);
                                if (null == entries) {
                                    Thread.sleep(100);
                                    continue;
                                }

                                for (FpqEntry entry : entries) {
                                    ByteBuffer bb = ByteBuffer.wrap(entry.getData());
                                    popSum.addAndGet(bb.getLong());
                                    if (entry.getId() % 500 == 0) {
                                        System.out.println("popped ID = " + entry.getId());
                                    }
                                }
                                fpq1.commit();
                                numPops.addAndGet(entries.size());
                                entries.clear();
                            }
                            finally {
                                if (fpq1.isTransactionActive()) {
                                    fpq1.rollback();
                                }
                            }
                            Thread.sleep(popRand.nextInt(10));
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

    private int pushEntries(int numEntries, Fpq fpq, int size) throws IOException {
        int sum = 0;
        for (int i=1;i <= numEntries;i++) {
            fpq.beginTransaction();
            fpq.push(createDataWithId(size, i));
            fpq.commit();
            sum += i;
        }
        assertThat(fpq.getNumberOfEntries(), is((long)numEntries));
        return sum;
    }

    private void checkSumByPopping(int expectedPops, int expectedSum) throws IOException {
        Fpq fpq = new Fpq();
        try {
            fpq.setQueueName("fpq2");
            fpq.setMaxTransactionSize(fpq1.getMaxTransactionSize());
            fpq.setMaxMemorySegmentSizeInBytes(fpq1.getMaxMemorySegmentSizeInBytes());
            fpq.setMaxJournalFileSize(fpq1.getMaxJournalFileSize());
            fpq.setMaxJournalDurationInMs(fpq1.getMaxJournalDurationInMs());
            fpq.setFlushPeriodInMs(fpq1.getFlushPeriodInMs());
            fpq.setNumberOfFlushWorkers(fpq1.getNumberOfFlushWorkers());
            fpq.setJournalDirectory(fpq1.getJournalDirectory());
            fpq.setPagingDirectory(fpq1.getPagingDirectory());
            fpq.init();
            checkSumByPopping(fpq, expectedPops, expectedSum);
        }
        finally {
            fpq.shutdown();
        }
    }

    private void checkSumByPopping(Fpq fpq, int expectedPops, int expectedSum) throws IOException {
        assertThat(fpq.getNumberOfEntries(), is((long)expectedPops));

        long end = System.currentTimeMillis() + 10000;
        int numPops = 0;
        int sum = 0;
        while (numPops < expectedPops && System.currentTimeMillis() < end) {
            fpq.beginTransaction();
            Collection<FpqEntry> entries = fpq.pop(1);
            fpq.commit();
            if (null != entries && !entries.isEmpty()) {
                numPops += entries.size();
                sum += extractId(entries.iterator().next().getData());
            }
            else {
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException e) {
                    // ignore
                    Thread.interrupted();
                }
            }
        }
        assertThat(numPops, is(expectedPops));
        assertThat(sum, is(expectedSum));
    }

    private byte[] createDataWithId(int size, int id) {
        return ByteBuffer.allocate(size).putInt(id).array();
    }

    private int extractId(byte[] data) {
        return ByteBuffer.wrap(data).getInt();
    }

    @Before
    public void setup() throws IOException {
        theDir = new File("tmp/junitTmp_"+ UUID.randomUUID().toString());
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
        try {
            fpq1.shutdown();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        FileUtils.deleteDirectory(theDir);
    }

}
