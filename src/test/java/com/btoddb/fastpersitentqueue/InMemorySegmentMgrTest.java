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
import java.util.Iterator;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;


/**
 *
 */
public class InMemorySegmentMgrTest {
    private static final Logger logger = LoggerFactory.getLogger(InMemorySegmentMgrTest.class);

    long numEntries = 30;
    InMemorySegmentMgr mgr;
    File theDir;

    @Test
    public void testPushCreatingMultipleSegments() throws Exception {
        mgr.init();

        for (int i=0;i < numEntries;i++) {
            mgr.push(new FpqEntry(new byte[100]));
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
        assertThat(seg.getNumberOfAvailableEntries(), is(0L));
        assertThat(seg.getQueue(), is(empty()));

        // this one is still active
        seg = iter.next();
        assertThat(seg.isPushingFinished(), is(false));
    }

    @Test
    public void testPop() throws Exception {
        mgr.init();

        for (int i=0;i < numEntries;i++) {
            mgr.push(new FpqEntry(String.format("%02d-3456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",i).getBytes()));
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

        assertThat(mgr.getSegments(), hasSize(1));
        assertThat(mgr.getNumberOfEntries(), is(0L));

        MemorySegment seg = mgr.getSegments().iterator().next();
        assertThat(seg.getStatus(), is(MemorySegment.Status.READY));
        assertThat(seg.getNumberOfAvailableEntries(), is(0L));
        assertThat(seg.getQueue(), is(empty()));

        assertThat(FileUtils.listFiles(theDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE), is(empty()));
    }

    @Test
    public void testPushPopPushPopEtc() throws Exception {
        int numEntries = 1000;
        mgr.init();

        for (int i=0;i < numEntries;i++) {
            mgr.push(new FpqEntry(String.format("%02d-3456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",i).getBytes()));
            mgr.push(new FpqEntry(String.format("%02d-3456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",i).getBytes()));
            mgr.pop();
            mgr.pop();
            assertThat("i="+i+" : segments", mgr.getSegments(), hasSize(1));
            assertThat("i="+i+" : entries", mgr.getNumberOfEntries(), is(0L));
        }

        assertThat(mgr.getSegments(), hasSize(1));
        assertThat(mgr.getNumberOfEntries(), is(0L));

        assertThat(FileUtils.listFiles(theDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE), is(empty()));
    }

    @Test
    public void testShutdown() throws Exception {
        mgr.init();
        for (int i=0;i < numEntries;i++) {
            mgr.push(new FpqEntry(String.format("%02d-3456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",i).getBytes()));
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
            mgr.push(new FpqEntry(String.format("%02d-3456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",i).getBytes()));
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
