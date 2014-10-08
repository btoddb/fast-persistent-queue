package com.btoddb.fastpersitentqueue.chronicle.plunkers;

import com.btoddb.fastpersitentqueue.chronicle.Config;
import com.btoddb.fastpersitentqueue.chronicle.FpqEvent;
import com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs.HdfsWriter;
import com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs.HdfsWriterFactory;
import com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs.WriterContext;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.NonStrictExpectations;
import mockit.Verifications;
import mockit.integration.junit4.JMockit;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.Future;

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
        assertThat(plunker.getKeyTokenizedFilePath().createFileName(event.getHeaders()), is(prefix+"the/dsp/path/file.avro"));
        assertThat(plunker.getPermTokenizedFilePath().createFileName(event.getHeaders()), is(prefix+"the/dsp/path/file.123.avro"));
        assertThat(plunker.getOpenTokenizedFilePath().createFileName(event.getHeaders()), is(prefix+"the/dsp/path/_file.avro.123.tmp"));
    }

    @Test
    public void testHandleEvent(
            @Mocked final HdfsWriterFactory writerFactory,
            @Mocked final HdfsWriter writer,
            @Mocked final Future<Void> future
            ) throws Exception {
        final FpqEvent event = new FpqEvent("the-body", true).addHeader("customer", "disney");

        new MockUp<WriterContext>() {
            @Mock
            public HdfsWriter getWriter() { return writer; }

            @Mock
            public Future<Void> getIdleFuture() { return future; }

            @Mock
            public void setIdleFuture(Future<Void> f) {
                assertThat(f, is(not(future)));
            }
        };

        new NonStrictExpectations() {{
            writerFactory.createWriter(anyString, anyString); times = 1; result = writer;

            writer.init(config); times = 1;
            writer.open(); times = 1;
            writer.write(event); times = 1;

            new WriterContext(writer); times = 1;

            future.cancel(false); times = 1;
        }};


        plunker.setWriterFactory(writerFactory);
        plunker.init(config);

        plunker.handleInternal(Collections.singleton(event));

        new Verifications() {{
        }};
    }
}