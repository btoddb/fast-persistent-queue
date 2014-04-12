package com.btoddb.fastpersitentqueue;

import com.eaio.uuid.UUID;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;


/**
 *
 */
public class JournalFileTest {
    File theDir;
    File theFile;
    AtomicLong idGen = new AtomicLong();

    @Test
    public void testInitForWritingThenClose() throws IOException {
        JournalFile jf1 = new JournalFile(theFile);
        jf1.initForWriting(new UUID());
        assertThat(jf1.isWriteMode(), is(true));
        assertThat(jf1.isOpen(), is(true));
        assertThat(jf1.getFilePosition(), is((long) JournalFile.HEADER_SIZE));
        jf1.close();

        assertThat(jf1.isOpen(), is(false));

        RandomAccessFile raFile = new RandomAccessFile(theFile, "rw");
        assertThat(raFile.readInt(), is(JournalFile.VERSION));
        assertThat(Utils.readUuidFromFile(raFile), is(jf1.getId()));
        assertThat(raFile.readLong(), is(0L));
        raFile.close();
    }

    @Test
    public void testInitForReadingThenClose() throws IOException {
        UUID id = new UUID();
        RandomAccessFile raFile = new RandomAccessFile(theFile, "rw");
        raFile.writeInt(1);
        Utils.writeUuidToFile(raFile, id);
        raFile.writeLong(123);
        raFile.close();

        JournalFile jf1 = new JournalFile(theFile);
        jf1.initForReading();
        assertThat(jf1.getVersion(), is(JournalFile.VERSION));
        assertThat(jf1.getId(), is(id));
        assertThat(jf1.getNumberOfEntries(), is(123L));

        assertThat(jf1.isOpen(), is(true));
        assertThat(jf1.isWriteMode(), is(false));
        assertThat(jf1.getFilePosition(), is((long)JournalFile.HEADER_SIZE));
        jf1.close();

        assertThat(jf1.isOpen(), is(false));
    }

    @Test
    public void testIsOpen() throws IOException {
        JournalFile jf1 = new JournalFile(theFile);
        jf1.initForWriting(new UUID());
        assertThat(jf1.getRandomAccessFile().getChannel().isOpen(), is(true));
        jf1.close();

        assertThat(jf1.getRandomAccessFile().getChannel().isOpen(), is(false));
    }

    @Test
    public void testAppendThenRead() throws Exception {
        String data = "my test data";
        JournalFile jf1 = new JournalFile(theFile);
        jf1.initForWriting(new UUID());
        FpqEntry entry1 = new FpqEntry(idGen.incrementAndGet(), data.getBytes());
        jf1.append(entry1);
        assertThat(jf1.getFilePosition(), is(JournalFile.HEADER_SIZE+12L+entry1.getData().length));
        jf1.close();

        JournalFile jf2 = new JournalFile(theFile);
        jf2.initForReading();
        assertThat(jf2.getVersion(), is(JournalFile.VERSION));
        assertThat(jf2.getId(), is(jf1.getId()));
        assertThat(jf2.getNumberOfEntries(), is(1L));
        assertThat(jf2.getFilePosition(), is((long) JournalFile.HEADER_SIZE));

        FpqEntry entry = jf2.readNextEntry();
        assertThat(jf2.getFilePosition(), is(jf2.getRandomAccessFile().length()));
        assertThat(entry.getData(), is(data.getBytes()));

        assertThat(jf2.readNextEntry(), is(nullValue()));
        jf2.close();
        assertThat(jf2.isOpen(), is(false));
    }

    @Test
    public void testReadNoData() throws Exception {
        JournalFile jf1 = new JournalFile(theFile);
        jf1.initForWriting(new UUID());
        jf1.close();

        JournalFile jf2 = new JournalFile(theFile);
        jf2.initForReading();
        assertThat(jf2.readNextEntry(), is(nullValue()));
        jf2.close();
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
        raFile.writeLong(1);
        raFile.writeLong(123);
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

    @Test
    public void testInvalidHeader() {
        fail();
    }

    @Test
    public void testIterator() throws Exception {
        int numEntries = 5;
        JournalFile jf1 = new JournalFile(theFile);
        jf1.initForWriting(new UUID());
        for (int i=0;i < numEntries;i++) {
            jf1.append(new FpqEntry(idGen.incrementAndGet(), String.valueOf(i).getBytes()));
        }
        jf1.close();

        JournalFile jf2 = new JournalFile(theFile);
        jf2.initForReading();
        int count = 0;
        for (FpqEntry entry : jf2) {
            assertThat(String.valueOf(count), is(new String(entry.getData())));
            count++;
        }

        assertThat(count, is(5));
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
