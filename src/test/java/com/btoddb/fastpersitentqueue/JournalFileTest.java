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
public class JournalFileTest {
    File theDir;
    File theFile;

    @Test
    public void testContstruction() throws IOException {
        JournalFile clf = new JournalFile(theFile);
        assertThat(clf.getWriterFilePosition(), is(0L));
        assertThat(clf.getReaderFilePosition(), is(0L));
        assertThat(clf.getRandomAccessWriterFile().getChannel().isOpen(), is(true));
        assertThat(clf.getRandomAccessReaderFile().getChannel().isOpen(), is(true));
    }

    @Test
    public void testClose() throws IOException {
        JournalFile clf = new JournalFile(theFile);
        clf.close();
        assertThat(clf.getRandomAccessWriterFile().getChannel().isOpen(), is(false));
        assertThat(clf.getRandomAccessReaderFile().getChannel().isOpen(), is(false));
    }

    @Test
    public void testAppendThenRead() throws Exception {
        String data = "my test data";
        JournalFile clf1 = new JournalFile(theFile);

        clf1.append(new Entry(data.getBytes()));
        assertThat(clf1.getWriterFilePosition(), is((long) JournalFile.VERSION_1_OVERHEAD+data.length()));

        assertThat(clf1.getReaderFilePosition(), is(0L));

        Entry entry = clf1.readNextEntry();
        assertThat(clf1.getReaderFilePosition(), is(clf1.getWriterFilePosition()));
        assertThat(entry.getVersion(), is(JournalFile.VERSION_1));
        assertThat(entry.getFilePosition(), is(0L));
        assertThat(entry.getData(), is(data.getBytes()));

        assertThat(clf1.readNextEntry(), is(nullValue()));
    }

    @Test
    public void testReadNoData() throws Exception {
        JournalFile clf1 = new JournalFile(theFile);
        assertThat(clf1.readNextEntry(), is(nullValue()));
    }

    @Test
    public void testReadBadVersion() throws Exception {
        try {
            JournalFile clf1 = new JournalFile(theFile);
            clf1.getRandomAccessWriterFile().writeInt(0);
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
            JournalFile clf1 = new JournalFile(theFile);
            clf1.getRandomAccessWriterFile().writeInt(1);
            clf1.getRandomAccessWriterFile().writeInt(1);
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
        FileUtils.forceMkdir(theDir);
        theFile = generateLogFileName();
    }

    @After
    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(theDir);
    }

    private File generateLogFileName() throws IOException {
        return File.createTempFile("junitTest", ".journal", theDir);
    }
}
