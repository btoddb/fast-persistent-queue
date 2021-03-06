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
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static junit.framework.TestCase.fail;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;


/**
 *
 */
public class InMemorySegmentMgrTest {
    private static final Logger logger = LoggerFactory.getLogger(InMemorySegmentMgrTest.class);

    AtomicLong idGen = new AtomicLong();
    long numEntries = 30;
    InMemorySegmentMgr mgr;
    File theDir;

    @Test
    public void testPushCreatingMultipleSegments() throws Exception {
        mgr.init();

        for (int i=0;i < numEntries;i++) {
            mgr.push(new FpqEntry(idGen.incrementAndGet(), new byte[100]));
        }

        long end = System.currentTimeMillis() + 1000;
        while (System.currentTimeMillis() < end && mgr.getNumberOfActiveSegments() > 4) {
            Thread.sleep(100);
        }
        assertThat(mgr.getSegments(), hasSize(5));
        assertThat(mgr.getNumberOfActiveSegments(), is(4));
        assertThat(mgr.getNumberOfEntries(), is(numEntries));
        Iterator<MemorySegment> iter = mgr.getSegments().iterator();
        for ( int i=0;i < 3;i++ ) {
            assertThat("i="+i+" : should be true", iter.next().isPushingFinished(), is(true));
        }
        // this one should be "offline"
        MemorySegment seg = iter.next();
        assertThat(seg.isPushingFinished(), is(true));
        assertThat(seg.getStatus(), is(MemorySegment.Status.OFFLINE));
        assertThat(seg.getNumberOfOnlineEntries(), is(7L));
        assertThat(seg.getNumberOfEntries(), is(7L));
        assertThat(seg.getQueue().keySet(), is(empty()));

        // this one is still active
        seg = iter.next();
        assertThat(seg.isPushingFinished(), is(false));
    }

    @Test
    public void testPop() throws Exception {
        mgr.init();

        for (int i=0;i < numEntries;i++) {
            mgr.push(new FpqEntry(idGen.incrementAndGet(), String.format("%02d-3456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",i).getBytes()));
        }

        long end = System.currentTimeMillis() + 1000;
        while (System.currentTimeMillis() < end && mgr.getNumberOfActiveSegments() > 4) {
            Thread.sleep(100);
        }

        for (int i=0;i < numEntries;i++) {
            FpqEntry entry;
            while (null == (entry=mgr.pop())) {
                Thread.sleep(100);
            }
            System.out.println(new String(entry.getData()));
        }

        // this triggers the last segment to be removed and gives it a chance to happen
        mgr.pop(1);
        Thread.sleep(500);

        assertThat(mgr.getNumberOfEntries(), is(0L));
        assertThat(mgr.getSegments(), hasSize(1));

        MemorySegment seg = mgr.getSegments().iterator().next();
        assertThat(seg.getStatus(), is(MemorySegment.Status.READY));
        assertThat(seg.getNumberOfOnlineEntries(), is(0L));
        assertThat(seg.getNumberOfEntries(), is(0L));
        assertThat(seg.getQueue().keySet(), is(empty()));

        assertThat(FileUtils.listFiles(theDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE), is(empty()));
    }

    @Test
    public void testShutdown() throws Exception {
        mgr.init();
        for (int i=0;i < numEntries;i++) {
            mgr.push(new FpqEntry(idGen.incrementAndGet(), String.format("%02d-3456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",i).getBytes()));
        }
        mgr.shutdown();

        assertThat(mgr.getSegments(), is(empty()));
        assertThat(mgr.getNumberOfActiveSegments(), is(0));
        assertThat(mgr.getNumberOfEntries(), is(0L));
        assertThat(FileUtils.listFiles(theDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE), hasSize(5));
    }

    @Test
    public void testLoadAtInit() throws Exception {
        // load and save some data
        mgr.init();
        for (int i=0;i < numEntries;i++) {
            mgr.push(new FpqEntry(idGen.incrementAndGet(), String.format("%02d-3456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",i).getBytes()));
        }
        mgr.shutdown();

        mgr = new InMemorySegmentMgr(null);
        mgr.setMaxSegmentSizeInBytes(1000);
        mgr.setPagingDirectory(theDir);
        mgr.init();

        assertThat(mgr.getSegments(), hasSize(6));
        assertThat(mgr.getNumberOfActiveSegments(), is(4));
        assertThat(mgr.getNumberOfEntries(), is(30L));
        assertThat(FileUtils.listFiles(theDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE), hasSize(5));
    }

    @Test
    public void testIsEntryQueuedWithPagedSegments() throws Exception {
        mgr.init();

        for (int i=1;i <= numEntries;i++) {
            mgr.push(new FpqEntry(idGen.incrementAndGet(), new byte[100]));
        }

        long end = System.currentTimeMillis() + 1000;
        while (System.currentTimeMillis() < end && mgr.getNumberOfActiveSegments() > 4) {
            Thread.sleep(100);
        }
        assertThat(mgr.getSegments(), hasSize(5));

        for (int i=1;i <= numEntries;i++) {
            assertThat("i = "+i+" (out of " + numEntries + ") should have been found", mgr.isEntryQueued(new FpqEntry(i, new byte[100])), is(true));
        }

        assertThat(mgr.isEntryQueued(new FpqEntry(numEntries+1, new byte[10])), is(false));
    }

    @Test
    public void testIsEntryQueuedNoPagedSegments() throws Exception {
        mgr.init();

        mgr.push(new FpqEntry(123, new byte[100]));
        assertThat(mgr.getSegments(), hasSize(1));

        assertThat(mgr.isEntryQueued(new FpqEntry(123, new byte[100])), is(true));
        assertThat(mgr.isEntryQueued(new FpqEntry(222, new byte[10])), is(false));
    }

    @Test
    public void testMaxSegmentSizeWithinRange() throws Exception {
        mgr.setMaxSegmentSizeInBytes(0);
        try {
            mgr.init();
        }
        catch (FpqException e) {
            assertThat(e.getMessage(), containsString("maxSegmentSizeInBytes"));
        }
    }

    @Test
    public void testMaxNumberOfActiveSegmentsWithinRange() throws Exception {
        mgr.setMaxSegmentSizeInBytes(3);
        try {
            mgr.init();
        }
        catch (FpqException e) {
            assertThat(e.getMessage(), containsString("maxNumberOfActiveSegments, must be 4 or greater"));
        }
    }

    @Test
    @Ignore("started but not finished yet")
    public void testIncreasingMaxNumberOfActiveSegments() throws Exception {
        mgr.setMaxNumberOfActiveSegments(10);
        mgr.init();

        fail();
    }

    @Test
    public void testThreading() throws IOException, ExecutionException {
        final int entrySize = 1000;
        final int numEntries = 3000;
        final int numPushers = 3;
        int numPoppers = 3;

        final Random pushRand = new Random(1000L);
        final Random popRand = new Random(1000000L);
        final AtomicInteger pusherFinishCount = new AtomicInteger();
        final AtomicInteger numPops = new AtomicInteger();
        final AtomicLong pushSum = new AtomicLong();
        final AtomicLong popSum = new AtomicLong();

        mgr.setMaxSegmentSizeInBytes(10000);
        mgr.init();

        ExecutorService execSrvc = Executors.newFixedThreadPool(numPushers + numPoppers);

        Set<Future> futures = new HashSet<Future>();

        // start pushing
        for (int i = 0; i < numPushers; i++) {
            Future future = execSrvc.submit(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < numEntries; i++) {
                        try {
                            long x = idGen.incrementAndGet();
                            pushSum.addAndGet(x);
                            FpqEntry entry = new FpqEntry(x, new byte[entrySize]);
                            mgr.push(entry);
                            if (x % 500 == 0) {
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
                    while (pusherFinishCount.get() < numPushers || !mgr.isEmpty()) {
                        try {
                            FpqEntry entry;
                            while (null != (entry=mgr.pop())) {
                                if (entry.getId() % 500 == 0) {
                                    System.out.println("popped ID = " + entry.getId());
                                }

                                popSum.addAndGet(entry.getId());
                                numPops.incrementAndGet();
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

        assertThat(numPops.get(), is(numEntries * numPushers));
        assertThat(popSum.get(), is(pushSum.get()));
        assertThat(mgr.getNumberOfEntries(), is(0L));
        assertThat(mgr.getNumberOfActiveSegments(), is(1));
        assertThat(mgr.getSegments(), hasSize(1));
        assertThat(FileUtils.listFiles(theDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE), is(empty()));

        // make sure we tested paging in/out
        assertThat(mgr.getNumberOfSwapOut(), is(greaterThan(0L)));
        assertThat(mgr.getNumberOfSwapIn(), is(mgr.getNumberOfSwapOut()));
    }

    @Test
    public void testSizeNeededGreaterThanMaxSegmentSize() throws Exception {
        mgr.init();

        try {
            mgr.push(new FpqEntry(1, new byte[1000]));
            fail("should have thrown FpqException because the size required to push events onto queue is larger than the max segment size");
        }
        catch (FpqException e) {
            assertThat(e.getMessage(), containsString("greater than maximum segment size"));
        }
    }
    // ---------

    @Before
    public void setup() throws IOException {
        theDir = new File("tmp/junitTmp_"+new UUID().toString());
        FileUtils.forceMkdir(theDir);

        mgr = new InMemorySegmentMgr(null);
        mgr.setMaxSegmentSizeInBytes(1000);
        mgr.setPagingDirectory(theDir);
    }


    @After
    public void cleanup() throws IOException {
        try {
            mgr.shutdown();
        }
        catch (Exception e) {
            // ignore
            logger.error("exception during test cleanup", e);
        }
        FileUtils.deleteDirectory(theDir);
    }


}
