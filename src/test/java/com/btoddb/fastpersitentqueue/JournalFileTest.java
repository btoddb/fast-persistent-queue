package com.btoddb.fastpersitentqueue;

import com.eaio.uuid.UUID;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

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
    public void testInitForWritingThenClose() throws IOException {
        JournalFile jf1 = new JournalFile(theFile);
        jf1.initForWriting(new UUID());
        assertThat(jf1.getRandomAccessReaderFile(), is(nullValue()));
        assertThat(jf1.getRandomAccessWriterFile().getChannel().isOpen(), is(true));
        assertThat(jf1.getWriterFilePosition(), is((long)JournalFile.HEADER_SIZE));
        jf1.close();

        assertThat(jf1.getRandomAccessWriterFile().getChannel().isOpen(), is(false));

        RandomAccessFile raFile = new RandomAccessFile(theFile, "rw");
        assertThat(raFile.readInt(), is(JournalFile.VERSION));
        assertThat(Utils.readUuidFromFile(raFile), is(jf1.getId()));
        raFile.close();
    }

    @Test
    public void testInitForReadingThenClose() throws IOException {
        UUID id = new UUID();
        RandomAccessFile raFile = new RandomAccessFile(theFile, "rw");
        raFile.writeInt(1);
        Utils.writeUuidToFile(raFile, id);
        raFile.close();

        JournalFile jf1 = new JournalFile(theFile);
        jf1.initForReading();
        assertThat(jf1.getRandomAccessReaderFile().getChannel().isOpen(), is(true));
        assertThat(jf1.getRandomAccessWriterFile(), is(nullValue()));
        assertThat(jf1.getReaderFilePosition(), is((long)JournalFile.HEADER_SIZE));
        jf1.close();

        assertThat(jf1.getRandomAccessReaderFile().getChannel().isOpen(), is(false));
    }

    @Test
    public void testCloseAfterOpenForRead() throws IOException {
        JournalFile jf1 = new JournalFile(theFile);
        jf1.initForWriting(new UUID());
        jf1.close();

        jf1 = new JournalFile(theFile);
        jf1.initForReading();
        jf1.close();
        assertThat(jf1.getRandomAccessReaderFile().getChannel().isOpen(), is(false));
    }

    @Test
    public void testAppendThenRead() throws Exception {
        String data = "my test data";
        JournalFile jf1 = new JournalFile(theFile);
        jf1.initForWriting(new UUID());
        FpqEntry entry1 = new FpqEntry(data.getBytes());
        jf1.append(entry1);
        assertThat(jf1.getWriterFilePosition(), is(JournalFile.HEADER_SIZE+entry1.getDiskSize()));
        jf1.close();

        jf1.initForReading();
        assertThat(jf1.getReaderFilePosition(), is((long)JournalFile.HEADER_SIZE));

        FpqEntry entry = jf1.readNextEntry();
        assertThat(jf1.getReaderFilePosition(), is(jf1.getRandomAccessReaderFile().length()));
        assertThat(entry.getData(), is(data.getBytes()));

        assertThat(jf1.readNextEntry(), is(nullValue()));
    }

    @Test
    public void testReadNoData() throws Exception {
        JournalFile jf1 = new JournalFile(theFile);
        jf1.initForWriting(new UUID());
        jf1.close();

        jf1.initForReading();
        assertThat(jf1.readNextEntry(), is(nullValue()));
    }

    @Test
    public void testReadBadVersion() throws Exception {
        UUID id = new UUID();
        RandomAccessFile raFile = new RandomAccessFile(theFile, "rw");
        raFile.writeInt(123);
        Utils.writeUuidToFile(raFile, id);
        raFile.close();

        try {
            JournalFile jf2 = new JournalFile(theFile);
            jf2.initForReading();
            fail("should have thrown " + FpqException.class.getSimpleName());
        }
        catch (FpqException e) {
            assertThat(e.getMessage(), containsString("does not match any expected version"));
        }
    }

    @Test
    public void testReadLengthOfEntryTooLarge() throws Exception {
        UUID id = new UUID();
        RandomAccessFile raFile = new RandomAccessFile(theFile, "rw");
        raFile.writeInt(1);
        Utils.writeUuidToFile(raFile, id);
        raFile.writeInt(12345);
        raFile.close();

        try {
            JournalFile jf1 = new JournalFile(theFile);
            jf1.initForReading();
            jf1.readNextEntry();
            fail("should have thrown " + FpqException.class.getSimpleName());
        }
        catch (FpqException e) {
            assertThat(e.getMessage(), containsString("entry length (12345) could not be satisfied"));
        }
    }

    // ---------------

    @Before
    public void setup() throws IOException {
        theDir = new File("junitTmp_"+new UUID().toString());
        FileUtils.forceMkdir(theDir);
        theFile = generateLogFileName();
    }

    @After
    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(theDir);
    }

    private File generateLogFileName() throws IOException {
        return new File(theDir, new UUID().toString());
    }
}
