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
import com.btoddb.fastpersitentqueue.chronicle.FpqEvent;
import com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs.HdfsWriter;
import com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs.HdfsWriterFactory;
import com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs.WriterContext;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import mockit.NonStrictExpectations;
import mockit.integration.junit4.JMockit;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;


@RunWith(JMockit.class)
public class HdfsPlunkerImplTest {
    HdfsPlunkerImpl plunker;
    Config config = new Config();
    File baseDir;


    @Before
    public void setup() throws Exception {
        baseDir = new File("tmp/" + UUID.randomUUID().toString());

        plunker = new HdfsPlunkerImpl();
        plunker.setPathPattern(String.format("file://%s/the/${customer}/path", baseDir.getPath()));
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
    public void testCreateFilePatterns() throws Exception {
        plunker.init(config);

        plunker.createFilePatterns();

        String prefix = "file://" + baseDir + "/";
        assertThat(plunker.getPathPattern(), is(prefix+"the/${customer}/path"));
        assertThat(plunker.getPermNamePattern(), is("file.avro"));
        assertThat(plunker.getOpenNamePattern(), is("_file.avro.tmp"));

        FpqEvent event = new FpqEvent("the-body", true).withHeader("customer", "dsp").withHeader("timestamp", "123");
        assertThat(plunker.getKeyTokenizedFilePath().createFileName(event.getHeaders()), is(prefix + "the/dsp/path/file.avro"));
        assertThat(plunker.getPermTokenizedFilePath().createFileName(event.getHeaders()), is(prefix + "the/dsp/path/file.123.avro"));
        assertThat(plunker.getOpenTokenizedFilePath().createFileName(event.getHeaders()), is(prefix + "the/dsp/path/_file.avro.123.tmp"));
    }

    @Test
    public void testInit(
            @Injectable final ScheduledThreadPoolExecutor closeExec, // don't want other executors affected
            @Injectable final ScheduledThreadPoolExecutor idleExec // don't want other executors affected
    ) throws Exception {
        new Expectations() {{
            idleExec.scheduleWithFixedDelay((Runnable) any, 10000, 10000, TimeUnit.MILLISECONDS); times = 1;
        }};
        plunker.setCloseExec(closeExec);
        plunker.setIdleTimerExec(idleExec);
        plunker.init(config);
    }

    @Test
    public void testShutdown(
            @Injectable final ScheduledThreadPoolExecutor closeExec, // don't want other executors affected
            @Injectable final ScheduledThreadPoolExecutor idleExec // don't want other executors affected
    ) throws Exception {
        new Expectations() {{
            idleExec.scheduleWithFixedDelay((Runnable) any, 10000, 10000, TimeUnit.MILLISECONDS); times = 1;
            idleExec.shutdown(); times = 1;
            closeExec.shutdown(); times = 1;
            closeExec.awaitTermination(plunker.getShutdownWaitTimeout(), TimeUnit.SECONDS); times = 1; result = true;
        }};
        plunker.setCloseExec(closeExec);
        plunker.setIdleTimerExec(idleExec);
        plunker.init(config);
        plunker.shutdown();
    }

    @Test
    public void testInitThenHandleEventThenShutdown(
            @Mocked final HdfsWriterFactory writerFactory,
            @Injectable final ScheduledThreadPoolExecutor closeExec, // don't want other executors affected
            @Mocked final WriterContext aContext,
            @Mocked final HdfsWriter aWriter,
            @Mocked final ScheduledFuture<Void> aFuture
    ) throws Exception {
        final List<FpqEvent> events = Arrays.asList(
                new FpqEvent("the-body", true).withHeader("msgId", "msg1").withHeader("customer", "customer1"),
                new FpqEvent("the-body", true).withHeader("msgId", "msg2").withHeader("customer", "customer2")
        );

        new NonStrictExpectations() {{
            for (int i=1;i <= 2;i++) {
                HdfsWriter writer = new HdfsWriter();
                writer.init(config); times = 1;
                writer.write(events.get(i - 1)); times = 1;
//                writer.close(); times = 1;

                writerFactory.createWriter(withSubstring("customer" + i), anyString); times = 1; result = writer;

                WriterContext context = new WriterContext(writer);
                context.getWriter(); times = 1; result = writer;

                context.readLock(); times = 1;
                context.readUnlock(); times = 1;
//                context.writeLock(); times = 1;
//                context.writeUnlock(); times = 1;

                closeExec.submit((Runnable) any); times = 2;
            }
        }};

        plunker.setWriterFactory(writerFactory);
        plunker.setCloseExec(closeExec);
        plunker.init(config);
        plunker.handleInternal(events);
        plunker.shutdown();
    }

    // ----------

    private class ScheduledFutureMock implements ScheduledFuture<Void> {
        @Override
        public long getDelay(TimeUnit unit) {
            return 0;
        }

        @Override
        public int compareTo(Delayed o) {
            return 0;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public Void get() throws InterruptedException, ExecutionException {
            return null;
        }

        @Override
        public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }
    }
}