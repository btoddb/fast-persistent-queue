package com.btoddb.fastpersitentqueue;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.fail;


/**
 *
 */
public class JournalFileMgrIT {
    File theDir;
    JournalFileMgr mgr;

    @Test
    public void testStart() throws Exception {
        mgr.start();

        assertThat(mgr.getJournalFiles().size(), is(1));

        JournalDescriptor jd = mgr.getJournalFiles().values().iterator().next();
        assertThat(jd.getFile().getFile().getParent(), is(theDir.getAbsolutePath()));
        assertThat(jd.getFile().getFile().exists(), is(true));
        assertThat(jd, sameInstance(mgr.getCurrentJournalDescriptor()));
    }

    @Test
    public void testJournalDirNameSetter() {
        mgr.setJournalDirName("/dirname");
        assertThat(mgr.getJournalDirName(), is("/dirname"));
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
        mgr.start();

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
    }

    @Test
    public void testJournalRollingBecauseOfTime() throws Exception {
        mgr.setMaxJournalFileSize(100);
        mgr.setMaxJournalDurationInMs(500);
        mgr.start();

        JournalDescriptor jd1 = mgr.getCurrentJournalDescriptor();
        mgr.append(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

        Thread.sleep(600);

        mgr.append(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        assertThat(jd1, not(sameInstance(mgr.getCurrentJournalDescriptor())));
        assertThat(jd1.isWritingFinished(), is(true));
        assertThat(mgr.getCurrentJournalDescriptor().isWritingFinished(), is(false));
        assertThat(mgr.getJournalFiles().entrySet(), hasSize(2));
    }

    @Test
    public void testAppend() throws IOException {
        mgr.start();
        byte[] data = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        mgr.append(data);

        long now = System.currentTimeMillis();
        assertThat(mgr.getCurrentJournalDescriptor().getStartTime(), is(greaterThanOrEqualTo(now)));
    }

    @Test
    public void testReportConsumption() throws IOException {
        mgr.start();
        Entry entry1 = mgr.append(new byte[] {0, 1});
        Entry entry2 = mgr.append(new byte[] {0, 1});
        Entry entry3 = mgr.append(new byte[] {0, 1});
        assertThat(mgr.getCurrentJournalDescriptor().getNumberOfUnconsumedEntries(), is(3));
        assertThat(mgr.getCurrentJournalDescriptor().getLastPositionRead(), is(-1L));

        mgr.reportTake(entry1);
        assertThat(mgr.getCurrentJournalDescriptor().getNumberOfUnconsumedEntries(), is(2));
        assertThat(mgr.getCurrentJournalDescriptor().getLastPositionRead(), is(entry1.getFilePosition()+entry1.getData().length+JournalFile.VERSION_1_OVERHEAD-1));

        mgr.reportTake(entry2);
        mgr.reportTake(entry3);

        assertThat(mgr.getCurrentJournalDescriptor().getNumberOfUnconsumedEntries(), is(0));
        assertThat(mgr.getCurrentJournalDescriptor().getLastPositionRead(), is(mgr.getCurrentJournalDescriptor().getFile().getLength() - 1));
    }

    @Test
    public void testAnyWritesHappened() throws IOException {
        mgr.start();
        assertThat(mgr.getCurrentJournalDescriptor().isAnyWritesHappened(), is(false));
        mgr.append(new byte[] {0, 1});
        assertThat(mgr.getCurrentJournalDescriptor().isAnyWritesHappened(), is(true));
    }

    @Test
    public void testShutdown() {
        fail("not implemented");
    }

    // --------------

    @Before
    public void setup() throws IOException {
        theDir = new File("junitTmp_"+ UUID.randomUUID().toString());
        FileUtils.forceMkdir(theDir);

        mgr = new JournalFileMgr();
        mgr.setJournalDirName(theDir.getAbsolutePath());
        mgr.setNumberOfFlushWorkers(4);
        mgr.setFlushPeriodInMs(1000);
    }

    @After
    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(theDir);
    }
}
