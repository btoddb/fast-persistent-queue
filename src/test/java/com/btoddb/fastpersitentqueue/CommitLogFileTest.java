package com.btoddb.fastpersitentqueue;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;


/**
 *
 */
public class CommitLogFileTest {
    File theDir;
    File theFile;

    @Test
    public void testContstruction() throws IOException {
        CommitLogFile clf = new CommitLogFile(theFile);
        assertThat(clf.getWriterFilePosition(), is(0L));
        assertThat(clf.getReaderFilePosition(), is(0L));
        assertThat(clf.getLength(), is(0L));
        assertThat(clf.getWriterFile().getChannel().isOpen(), is(true));
        assertThat(clf.getReaderFile().getChannel().isOpen(), is(true));
    }

    @Test
    public void testClose() throws IOException {
        CommitLogFile clf = new CommitLogFile(theFile);
        clf.close();
        assertThat(clf.getWriterFile().getChannel().isOpen(), is(false));
        assertThat(clf.getReaderFile().getChannel().isOpen(), is(false));
    }

    @Test
    public void testAppendThenRead() throws Exception {
        String data = new String("my test data");
        CommitLogFile clf1 = new CommitLogFile(theFile);

        clf1.append(new Entry(CommitLogFile.VERSION_1, data.getBytes(), 0));
        assertThat(clf1.getWriterFilePosition(), is((long)CommitLogFile.VERSION_1_OVERHEAD+data.length()));

        assertThat(clf1.getReaderFilePosition(), is(0L));
        assertThat(clf1.getLength(), is((long)CommitLogFile.VERSION_1_OVERHEAD+data.length()));

        Entry entry = clf1.readNextEntry();
        assertThat(clf1.getReaderFilePosition(), is(clf1.getLength()));
        assertThat(entry.getVersion(), is(CommitLogFile.VERSION_1));
        assertThat(entry.getFilePosition(), is(0L));
        assertThat(entry.getData(), is(data.getBytes()));

        assertThat(clf1.readNextEntry(), is(nullValue()));
    }

    @Test
    public void testReadNoData() throws Exception {
        CommitLogFile clf1 = new CommitLogFile(theFile);
        assertThat(clf1.readNextEntry(), is(nullValue()));
    }

    @Test
    public void testReadBadVersion() throws Exception {
        try {
            CommitLogFile clf1 = new CommitLogFile(theFile);
            clf1.getWriterFile().writeInt(0);
            clf1.readNextEntry();
            fail("should have thrown QueryException");
        }
        catch (QueueException e) {
            assertThat(e.getMessage(), startsWith("invalid version"));
        }
    }

    @Test
    public void testReadBadLength() throws Exception {
        try {
            CommitLogFile clf1 = new CommitLogFile(theFile);
            clf1.getWriterFile().writeInt(1);
            clf1.getWriterFile().writeInt(1);
            clf1.readNextEntry();
            fail("should have thrown QueryException");
        }
        catch (QueueException e) {
            assertThat(e.getMessage(), containsString("entry length (1) could not be satisfied"));
        }
    }

    // ---------------

    @Before
    public void setup() throws IOException {
        theDir = new File("junitTmp_"+ UUID.randomUUID().toString());
        theDir.mkdir();
        theFile = generateLogFileName();
    }

    @After
    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(theDir);
    }

    private File generateLogFileName() throws IOException {
        return File.createTempFile("junitTest", ".commitlog", theDir);
    }
}
