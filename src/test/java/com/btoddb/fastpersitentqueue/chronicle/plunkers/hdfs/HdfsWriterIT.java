package com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs;

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
import com.btoddb.fastpersitentqueue.chronicle.serializers.JsonSerializerImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;


public class HdfsWriterIT {
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
        FpqEvent event = new FpqEvent("hello-world!").withHeader("customer", "dsp");

        writer.init(config);
        writer.write(event);

        // data isn't flushed to file until closed, so can't check for actual events in 'open' file
        // ... and it doesn't matter if you call flush/sync
        assertThat(new File(writer.getFileDescriptor().getPermFilename()), not(ftUtils.exists()));
        assertThat(new File(writer.getFileDescriptor().getOpenFilename()), ftUtils.exists());

        writer.close();

        assertThat(new File(writer.getFileDescriptor().getPermFilename()), ftUtils.hasEvent(event));
        assertThat(new File(writer.getFileDescriptor().getOpenFilename()), not(ftUtils.exists()));
    }
}