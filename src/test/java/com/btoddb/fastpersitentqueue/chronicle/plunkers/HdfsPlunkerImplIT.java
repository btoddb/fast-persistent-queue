package com.btoddb.fastpersitentqueue.chronicle.plunkers;

import com.btoddb.fastpersitentqueue.chronicle.Config;
import com.btoddb.fastpersitentqueue.chronicle.FileTestUtils;
import com.btoddb.fastpersitentqueue.chronicle.FpqEvent;
import com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs.HdfsFileDescriptor;
import com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs.WriterContext;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
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
                new FpqEvent("the-body", true).withHeader("msgId", "1").withHeader("customer", "cust-one"),
                new FpqEvent("the-body", true).withHeader("msgId", "2").withHeader("customer", "cust-two")
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
                new FpqEvent("the-body", true).withHeader("msgId", "1").withHeader("customer", "cust-one"),
                new FpqEvent("the-body", true).withHeader("msgId", "2").withHeader("customer", "cust-two")
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
        plunker.handleInternal(Arrays.asList(new FpqEvent[] {new FpqEvent("the-body", true).withHeader("msgId", "2").withHeader("customer", "cust-one")}));

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
                    new FpqEvent("the-body", true).withHeader("customer", "cust").withHeader("msgId", String.valueOf(i))}
            ));
            Thread.sleep(200);
        }

        // gives writer a chance to roll
        Thread.sleep(1500);

        // send another to make a new file
        plunker.handleInternal(Arrays.asList(new FpqEvent[] {
                    new FpqEvent("the-body", true).withHeader("customer", "cust").withHeader("msgId", String.valueOf("odd"))}
        ));

        assertThat(new File(String.format("%s/the/cust/path", baseDir.getPath())), ftUtils.numWithSuffix(".tmp", 1));
        assertThat(new File(String.format("%s/the/cust/path", baseDir.getPath())), ftUtils.numWithSuffix(".avro", 1));

        plunker.shutdown();
    }
}
