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
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;


public class HdfsPlunkerImplIT {
    FileTestUtils ftUtils;
    HdfsPlunkerImpl plunker;
    Config config = new Config();
    File baseDir;


    @Before
    public void setup() throws Exception {
        baseDir = new File("tmp/" + UUID.randomUUID().toString());

        ftUtils = new FileTestUtils(config);

        plunker = new HdfsPlunkerImpl();
        plunker.setPathPattern(String.format("%s/the/${customer}/path", baseDir.getPath()));
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
    public void testSingleEvent() throws Exception {
        FpqEvent event = new FpqEvent("the-body", true).withHeader("customer", "cust-one");
        plunker.init(config);
        plunker.handleInternal(Collections.singleton(event));
        plunker.shutdown();

        Collection<WriterContext> writers = plunker.getWriters();
        HdfsFileDescriptor desc = writers.iterator().next().getWriter().getFileDescriptor();

        assertThat(new File(desc.getPermFilename()), ftUtils.hasEvent(event));
        assertThat(new File(desc.getOpenFilename()), not(ftUtils.exists()));

    }
}
