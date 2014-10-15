package com.btoddb.fastpersitentqueue.chronicle.plunkers;

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

import com.btoddb.fastpersitentqueue.chronicle.Config;
import com.btoddb.fastpersitentqueue.chronicle.FileTestUtils;
import com.btoddb.fastpersitentqueue.chronicle.FpqEvent;
import com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs.HdfsFileDescriptor;
import com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs.WriterContext;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;


public class HdfsPlunkerImplIT {
    FileTestUtils ftUtils;
    HdfsPlunkerImpl plunker;
    Config config = new Config();
    File baseDir;
    String dirPattern;


    @Before
    public void setup() throws Exception {
        baseDir = new File("tmp/" + UUID.randomUUID().toString());

        dirPattern = String.format("%s/the/${customer}/path", baseDir.getPath());
        ftUtils = new FileTestUtils(config);

        plunker = new HdfsPlunkerImpl();
        plunker.setPathPattern(dirPattern);
        plunker.setPermNamePattern("file.avro");
        plunker.setOpenNamePattern("_file.avro.tmp");
    }

    @After
    public void cleanup() {
        try {
            FileUtils.deleteDirectory(baseDir);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testHandlingEvents() throws Exception {
        FpqEvent[] events = new FpqEvent[] {
                new FpqEvent("the-body").withHeader("msgId", "1").withHeader("customer", "cust-one"),
                new FpqEvent("the-body").withHeader("msgId", "2").withHeader("customer", "cust-two")
        };
        plunker.init(config);
        plunker.handleInternal(Arrays.asList(events));
        plunker.shutdown();

        for (WriterContext context : plunker.getWriters()) {
            HdfsFileDescriptor desc = context.getWriter().getFileDescriptor();

            assertThat(new File(desc.getPermFilename()), ftUtils.hasCount(1));
            assertThat(new File(desc.getOpenFilename()), not(ftUtils.exists()));
        }
    }

    @Test
    public void testIdleTimeout() throws Exception {
        FpqEvent[] events = new FpqEvent[] {
                new FpqEvent("the-body").withHeader("msgId", "1").withHeader("customer", "cust-one"),
                new FpqEvent("the-body").withHeader("msgId", "2").withHeader("customer", "cust-two")
        };
        plunker.setIdleTimeout(1);
        plunker.setTimeoutCheckPeriod(1);
        plunker.init(config);
        plunker.handleInternal(Arrays.asList(events));

        long endTime = System.currentTimeMillis()+5000;
        while (!plunker.getWriters().isEmpty() && System.currentTimeMillis() < endTime) {
            Thread.sleep(200);
        }

        assertThat(plunker.getWriters(), is(empty()));

        plunker.shutdown();
    }

    @Test
    public void testNoIdleTimeout() throws Exception {
        plunker.setIdleTimeout(0);
        plunker.setRollPeriod(60);
        plunker.setTimeoutCheckPeriod(1);
        plunker.init(config);
        plunker.handleInternal(Arrays.asList(new FpqEvent[] {new FpqEvent("the-body").withHeader("msgId", "2").withHeader("customer", "cust-one")}));

        long endTime = System.currentTimeMillis()+3000;
        while (System.currentTimeMillis() < endTime) {
            Thread.sleep(200);
        }

        assertThat(plunker.getWriters(), is(not(empty())));

        plunker.shutdown();
    }

    @Test
    public void testRollTimeout() throws Exception {
        plunker.setIdleTimeout(0);
        plunker.setRollPeriod(2);
        plunker.setTimeoutCheckPeriod(1);
        plunker.init(config);

        // should take ~2.2 seconds - which forces the writer to roll
        for (int i=0;i < 11;i++) {
            plunker.handleInternal(Arrays.asList(new FpqEvent[] {
                    new FpqEvent("the-body").withHeader("customer", "cust").withHeader("msgId", String.valueOf(i))}
            ));
            Thread.sleep(200);
        }

        // gives writer a chance to roll
        Thread.sleep(1500);

        // send another to make a new file
        plunker.handleInternal(Arrays.asList(new FpqEvent[] {
                    new FpqEvent("the-body").withHeader("customer", "cust").withHeader("msgId", String.valueOf("odd"))}
        ));

        assertThat(new File(String.format("%s/the/cust/path", baseDir.getPath())), ftUtils.countWithSuffix(".tmp", 1));
        assertThat(new File(String.format("%s/the/cust/path", baseDir.getPath())), ftUtils.countWithSuffix(".avro", 1));

        plunker.shutdown();
    }

    @Test
    public void testLongRun() throws Exception {
        plunker.setIdleTimeout(0);
        plunker.setRollPeriod(2);
        plunker.setTimeoutCheckPeriod(100);
        plunker.init(config);

        final int sleep = 200;
        final int maxCount = 100; // 20 seconds at 'sleep' interval should be 10
        final AtomicInteger count = new AtomicInteger();

        plunker.handleInternal(Arrays.asList(new FpqEvent[] {
                                                     new FpqEvent("the-body").withHeader("customer", "cust").withHeader("msgId", String.valueOf(count.getAndIncrement()))
                                             }
        ));

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        System.out.println("time = " + System.currentTimeMillis());
                        plunker.handleInternal(Arrays.asList(new FpqEvent[] {
                                  new FpqEvent("the-body").withHeader("customer", "cust").withHeader("msgId", String.valueOf(count.get()))
                                  }
                        ));
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }

                    count.incrementAndGet();
                }
            },
            0, sleep, TimeUnit.MILLISECONDS);

        while (count.get() < maxCount) {
            Thread.sleep(sleep/2);
        }

        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);

        Thread.sleep(1500);
        plunker.shutdown();

        FpqEvent[] events = new FpqEvent[count.get()];
        for (int i=0;i < count.get();i++) {
            events[i] = new FpqEvent("the-body").withHeader("customer", "cust").withHeader("msgId", String.valueOf(i));
        }

        File theDir = new File(String.format("%s/the/cust/path", baseDir.getPath()));

        assertThat(theDir, ftUtils.countWithSuffix(".tmp", 0));
        assertThat(theDir, ftUtils.countWithSuffix(".avro", 10));

        assertThat(theDir, ftUtils.hasEventsInDir(events));
    }
}
