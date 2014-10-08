package com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs;

import com.btoddb.fastpersitentqueue.chronicle.Config;
import com.btoddb.fastpersitentqueue.chronicle.FileTestUtils;
import com.btoddb.fastpersitentqueue.chronicle.FpqEvent;
import com.btoddb.fastpersitentqueue.chronicle.serializers.JsonSerializerImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;


public class HdfsWriterTest {
    FileTestUtils ftUtils;
    File baseDir;
    Config config = new Config();
    HdfsWriter writer;

    @Before
    public void setup() {
        baseDir = new File("tmp/" + UUID.randomUUID().toString());
        ftUtils = new FileTestUtils(config);

        writer = new HdfsWriter();
        writer.setSerializer(new JsonSerializerImpl(config));
        writer.setPermFilenamePattern(new File(baseDir, "file-${timestamp}.avro").getPath());
        writer.setOpenFilenamePattern(new File(baseDir, "_file-${timestamp}.avro.tmp").getPath());
    }

    @After
    public void cleanup() {
        try {
            org.apache.commons.io.FileUtils.deleteDirectory(baseDir);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testOpenWriteClose() throws Exception {
        FpqEvent event = new FpqEvent("hello-world!", true).addHeader("customer", "dsp");

        writer.init(config);

        writer.open();
        writer.write(event);
        writer.close();

        assertThat(new File(writer.getCurrentFilename()), ftUtils.hasEvents(new FpqEvent[] {event}));
    }
}