package com.btoddb.fastpersitentqueue;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;


/**
 *
 */
public class FpqIT {
    File theDir;
    Fpq q;

    @Test
    public void testPushNoCommit() throws Exception {
        q.init();

        FpqContext ctxt = q.createContext();
        q.push(ctxt, new byte[10]);
        q.push(ctxt, new byte[10]);
        q.push(ctxt, new byte[10]);

        assertThat(ctxt.isPushing(), is(true));
        assertThat(ctxt.isPopping(), is(false));
        assertThat(ctxt.getQueue(), hasSize(3));
        assertThat(q.getMemoryMgr().size(), is(0L));

        q.commit(ctxt);

        assertThat(ctxt.isPushing(), is(false));
        assertThat(ctxt.isPopping(), is(false));
        assertThat(ctxt.getQueue(), is(nullValue()));
        assertThat(q.getMemoryMgr().size(), is(3L));
        assertThat(q.getJournalFileMgr().getCurrentJournalDescriptor().getNumberOfUnconsumedEntries(), is(3L));
    }

    @Test
    public void testPushExceedTxMax() throws Exception {
        q.setMaxTransactionSize(2);
        q.init();

        FpqContext ctxt = q.createContext();
        q.push(ctxt, new byte[10]);
        q.push(ctxt, new byte[10]);

        try {
            q.push(ctxt, new byte[10]);
            fail("should have thrown exception because of exceeding transaction size");
        }
        catch (FpqException e) {
            // yay!!
        }

    }

    @Test
    public void testPop() throws Exception {
        q.init();

        FpqContext ctxt = q.createContext();
        q.push(ctxt, new byte[10]);
        q.push(ctxt, new byte[10]);
        q.push(ctxt, new byte[10]);
        q.commit(ctxt);

        assertThat(q.getMemoryMgr().size(), is(3L));

        Collection<FpqEntry> entries = q.pop(ctxt, q.getMaxTransactionSize());

        assertThat(entries, hasSize(3));
        assertThat(ctxt.isPushing(), is(false));
        assertThat(ctxt.isPopping(), is(true));
        assertThat(ctxt.getQueue(), hasSize(3));
        assertThat(q.getMemoryMgr().size(), is(0L));
        assertThat(q.getJournalFileMgr().getCurrentJournalDescriptor().getNumberOfUnconsumedEntries(), is(3L));

        q.commit(ctxt);

        assertThat(ctxt.isPushing(), is(false));
        assertThat(ctxt.isPopping(), is(false));
        assertThat(ctxt.getQueue(), is(nullValue()));
        assertThat(q.getMemoryMgr().size(), is(0L));
        assertThat(q.getJournalFileMgr().getCurrentJournalDescriptor().getNumberOfUnconsumedEntries(), is(0L));
    }

    @Test
    public void testThreading() throws Exception {
        q.setMaxMemorySegmentSizeInBytes(10000);
        q.setMaxJournalFileSize(1000);
        q.setMaxTransactionSize(100);

        final int numEntries = 10000;
        final int numPushers = 3;
        int numPoppers = 2;
        final int popBatchSize = 100;

        final Random pushRand = new Random(1000L);
        final Random popRand = new Random(1000000L);
        final AtomicInteger pusherFinishCount = new AtomicInteger();
        final AtomicInteger numPops = new AtomicInteger();

        q.init();

        ExecutorService execSrvc = Executors.newFixedThreadPool(numPushers + numPoppers);

        Set<Future> futures = new HashSet<Future>();

        // start pushing
        for (int i = 0; i < numPushers; i++) {
            Future future = execSrvc.submit(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < numEntries; i++) {
                        try {
                            FpqContext context = q.createContext();
                            q.push(context, new byte[100]);
                            q.commit(context);
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
                    while (pusherFinishCount.get() < numPushers || !q.isEmpty()) {
                        try {
                            FpqContext context = q.createContext();
                            Collection<FpqEntry> entries;
                            while (null != (entries=q.pop(context, popBatchSize))) {
                                numPops.addAndGet(entries.size());
                                q.commit(context);
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
        assertThat(q.getNumberOfEntries(), is(0L));
        assertThat(q.getMemoryMgr().getNumberOfActiveSegments(), is(1));
        assertThat(q.getMemoryMgr().getSegments(), hasSize(1));
        assertThat(q.getJournalFileMgr().getJournalFiles().entrySet(), hasSize(1));
        assertThat(FileUtils.listFiles(q.getPagingDirectory(), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE), is(empty()));
        assertThat(FileUtils.listFiles(q.getJournalDirectory(), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE), hasSize(1));
    }

    // --------------

    @Before
    public void setup() throws IOException {
        theDir = new File("junitTmp_"+ UUID.randomUUID().toString());
        FileUtils.forceMkdir(theDir);

        q = new Fpq();
        q.setMaxMemorySegmentSizeInBytes(10000);
        q.setMaxTransactionSize(100);
        q.setJournalDirectory(new File(theDir, "journal"));
        q.setPagingDirectory(new File(theDir, "paging"));

        FileUtils.forceMkdir(q.getJournalDirectory());
        FileUtils.forceMkdir(q.getPagingDirectory());
    }

    @After
    public void cleanup() throws IOException {
        q.shutdown();
        FileUtils.deleteDirectory(theDir);
    }

}
