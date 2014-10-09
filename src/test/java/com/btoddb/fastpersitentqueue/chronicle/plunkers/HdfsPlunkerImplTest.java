package com.btoddb.fastpersitentqueue.chronicle.plunkers;

import com.btoddb.fastpersitentqueue.chronicle.Config;
import com.btoddb.fastpersitentqueue.chronicle.FpqEvent;
import com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs.HdfsWriter;
import com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs.HdfsWriterFactory;
import com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs.WriterContext;
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
import static org.hamcrest.Matchers.not;


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

        FpqEvent event = new FpqEvent("the-body", true).addHeader("customer", "dsp").addHeader("timestamp", "123");
        assertThat(plunker.getKeyTokenizedFilePath().createFileName(event.getHeaders()), is(prefix + "the/dsp/path/file.avro"));
        assertThat(plunker.getPermTokenizedFilePath().createFileName(event.getHeaders()), is(prefix + "the/dsp/path/file.123.avro"));
        assertThat(plunker.getOpenTokenizedFilePath().createFileName(event.getHeaders()), is(prefix + "the/dsp/path/_file.avro.123.tmp"));
    }

    @Test
    public void testHandleEvent(
            @Mocked final HdfsWriterFactory writerFactory,
            @Mocked final ScheduledThreadPoolExecutor idleExec,
            @Mocked final WriterContext aContext,
            @Mocked final HdfsWriter aWriter,
            @Mocked final ScheduledFuture<Void> aFuture
    ) throws Exception {
        final List<FpqEvent> events = Arrays.asList(
                new FpqEvent("the-body", true).addHeader("msgId", "msg1").addHeader("customer", "customer1"),
                new FpqEvent("the-body", true).addHeader("msgId", "msg2").addHeader("customer", "customer2")
        );

        new NonStrictExpectations() {{
            for (int i=1;i <= 2;i++) {
                HdfsWriter writer = new HdfsWriter();
                writer.init(config); times = 1;
                writer.open(); times = 1;
                writer.write(events.get(i-1)); times = 1;

                writerFactory.createWriter(withSubstring("customer" + i), anyString); times = 1; result = writer;

                ScheduledFuture<Void> future = new ScheduledFutureMock();
                future.cancel(false); times = 1;

                WriterContext context = new WriterContext(writer);
                context.getWriter(); times = 1; result = writer;
                context.getIdleFuture(); times = 2; result = future;

                idleExec.schedule((Runnable) any, plunker.getIdleTimeout(), TimeUnit.SECONDS); times = 2;
            }
        }};

        plunker.setWriterFactory(writerFactory);
        plunker.setIdleTimerExec(idleExec);
        plunker.init(config);
        plunker.handleInternal(events);
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