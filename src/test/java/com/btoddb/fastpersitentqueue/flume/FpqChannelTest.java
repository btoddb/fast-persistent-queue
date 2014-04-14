package com.btoddb.fastpersitentqueue.flume;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.event.SimpleEvent;
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;


/**
 *
 */
public class FpqChannelTest {
    File theDir;
    FpqChannel channel;

    @Test
    public void testConfigure() throws Exception {
        FpqChannel channel = new FpqChannel();

        Context context = new Context();
        context.put("maxJournalFileSize", "1");
        context.put("numberOfFlushWorkers", "2");
        context.put("flushPeriodInMs", "3");
        context.put("maxJournalDurationInMs", "4");
        context.put("journalDirectory", "j/d");
        context.put("maxMemorySegmentSizeInBytes", "5");
        context.put("pagingDirectory", "p/d");
        context.put("transactionCapacity", "6");

        channel.configure(context);

        assertThat(channel.getMaxJournalFileSize(), is(1L));
        assertThat(channel.getNumberOfFlushWorkers(), is(2));
        assertThat(channel.getFlushPeriodInMs(), is(3L));
        assertThat(channel.getMaxJournalDurationInMs(), is(4L));
        assertThat(channel.getJournalDirectory(), is(new File("j/d")));
        assertThat(channel.getMaxMemorySegmentSizeInBytes(), is(5L));
        assertThat(channel.getPagingDirectory(), is(new File("p/d")));
        assertThat(channel.getMaxTransactionSize(), is(6));
    }

    @Test
    public void testPutCommitTakeOneEvent() throws Exception {
        channel.start();

        Transaction tx = channel.getTransaction();
        tx.begin();
        MyEvent event1 = new MyEvent();
        event1.addHeader("h1", "v1")
                .addHeader("h2", "v2")
                .setBody("muh body".getBytes());
        channel.put(event1);
        tx.commit();
        tx.close();

        tx = channel.getTransaction();
        tx.begin();
        Event event2 = channel.take();
        tx.commit();
        tx.close();

        assertThat(event2.getHeaders(), is(event1.getHeaders()));
        assertThat(event2.getBody(), is(event1.getBody()));
    }

    @Test
    public void testPutCommitTakeManyEvents() throws Exception {
        int numEvents = 5000;
        Transaction tx = null;

        channel.setMaxMemorySegmentSizeInBytes(10000);
        channel.setMaxJournalFileSize(10000);
        channel.start();

        for (int i=1;i <= numEvents;i++) {
            if (0 == (i-1) % 100) {
                tx = channel.getTransaction();
                tx.begin();
            }
            MyEvent event1 = new MyEvent();
            event1.addHeader("h1", "v1")
                    .addHeader("h2", "v2")
                    .setBody(String.format("%d = muh body", i).getBytes());
            channel.put(event1);
            if (0 == i % 100) {
                tx.commit();
                tx.close();
            }
        }

        tx = channel.getTransaction();
        tx.begin();
        Event event2 = channel.take();
        tx.commit();
        tx.close();

        fail();
//        assertThat(event2.getHeaders(), is(event1.getHeaders()));
//        assertThat(event2.getBody(), is(event1.getBody()));
    }

    @Test
    public void testTakeEmpty() throws Exception {
        channel.start();

        Transaction tx = channel.getTransaction();
        tx.begin();
        Event event = channel.take();
        tx.commit();
        tx.close();

        assertThat(event, is(nullValue()));
    }

    @Test
    public void testThreading() throws Exception {
        final int numEntries = 10000;
        final int numPushers = 4;
        final int numPoppers = 4;
        final int entrySize = 1000;
        channel.setMaxTransactionSize(2000);
        final int popBatchSize = 100;
        channel.setMaxMemorySegmentSizeInBytes(10000000);
        channel.setMaxJournalFileSize(10000000);
        channel.setMaxJournalDurationInMs(30000);
        channel.setFlushPeriodInMs(1000);
        channel.setNumberOfFlushWorkers(4);

        final Random pushRand = new Random(1000L);
        final Random popRand = new Random(1000000L);
        final AtomicInteger pusherFinishCount = new AtomicInteger();
        final AtomicInteger numPops = new AtomicInteger();
        final AtomicLong counter = new AtomicLong();
        final AtomicLong pushSum = new AtomicLong();
        final AtomicLong popSum = new AtomicLong();

        channel.start();

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

                            Transaction tx = channel.getTransaction();
                            tx.begin();
                            MyEvent event1 = new MyEvent();
                            event1.addHeader("x", String.valueOf(x))
                                    .setBody(new byte[numEntries - 8]); // take out size of long
                            channel.put(event1);
                            tx.commit();
                            tx.close();

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
                    while (pusherFinishCount.get() < numPushers || !channel.isEmpty()) {
                        try {
                            Transaction tx = channel.getTransaction();
                            tx.begin();

                            Event event;
                            int count = popBatchSize;
                            while (null != (event=channel.take()) && count-- > 0) {
                                popSum.addAndGet(Long.valueOf(event.getHeaders().get("x")));
                                numPops.incrementAndGet();
                            }

                            tx.commit();
                            tx.close();

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
        assertThat(channel.isEmpty(), is(true));
        assertThat(pushSum.get(), is(popSum.get()));
    }

    // ------------------

    @Before
    public void setup() throws IOException {
        theDir = new File("junitTmp_"+ UUID.randomUUID().toString()).getCanonicalFile();
        FileUtils.forceMkdir(theDir);

        channel = new FpqChannel();
        channel.setMaxJournalFileSize(10000);
        channel.setNumberOfFlushWorkers(1);
        channel.setFlushPeriodInMs(1000);
        channel.setMaxJournalDurationInMs(10000);
        channel.setJournalDirectory(new File(theDir, "journal"));
        channel.setMaxMemorySegmentSizeInBytes(1000);
        channel.setPagingDirectory(new File(theDir, "paging"));
        channel.setMaxTransactionSize(2000);
    }

    @After
    public void cleanup() throws IOException {
        channel.stop();
        FileUtils.deleteDirectory(theDir);
    }

    // --------

    class MyEvent extends SimpleEvent {
        public MyEvent addHeader(String name, String value) {
            getHeaders().put(name, value);
            return this;
        }
    }
}
