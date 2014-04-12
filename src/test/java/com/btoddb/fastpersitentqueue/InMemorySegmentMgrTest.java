package com.btoddb.fastpersitentqueue;

import com.eaio.uuid.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
        assertThat(seg.getNumberOfAvailableEntries(), is(7L));
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

        assertThat(mgr.getNumberOfEntries(), is(0L));
        assertThat(mgr.getSegments(), hasSize(1));

        MemorySegment seg = mgr.getSegments().iterator().next();
        assertThat(seg.getStatus(), is(MemorySegment.Status.READY));
        assertThat(seg.getNumberOfAvailableEntries(), is(0L));
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

        mgr = new InMemorySegmentMgr();
        mgr.setMaxSegmentSizeInBytes(1000);
        mgr.setPagingDirectory(theDir);
        mgr.init();

        assertThat(mgr.getSegments(), hasSize(6));
        assertThat(mgr.getNumberOfActiveSegments(), is(4));
        assertThat(mgr.getNumberOfEntries(), is(30L));
        assertThat(FileUtils.listFiles(theDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE), hasSize(2));
    }


    @Test
    public void testThreading() throws IOException, ExecutionException {
        final int numEntries = 10000;
        final int numPushers = 3;
        int numPoppers = 3;

        final Random pushRand = new Random(1000L);
        final Random popRand = new Random(1000000L);
        final AtomicInteger pusherFinishCount = new AtomicInteger();
        final AtomicInteger numPops = new AtomicInteger();

        mgr.setMaxSegmentSizeInBytes(1000);
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
                            FpqEntry entry = new FpqEntry(idGen.incrementAndGet(), new byte[100]);
                            mgr.push(entry);
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
        assertThat(mgr.getNumberOfEntries(), is(0L));
        assertThat(mgr.getNumberOfActiveSegments(), is(1));
        assertThat(mgr.getSegments(), hasSize(1));
        assertThat(FileUtils.listFiles(theDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE), is(empty()));
    }

    // ---------

    @Before
    public void setup() throws IOException {
        theDir = new File("junitTmp_"+new UUID().toString());
        FileUtils.forceMkdir(theDir);

        mgr = new InMemorySegmentMgr();
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
