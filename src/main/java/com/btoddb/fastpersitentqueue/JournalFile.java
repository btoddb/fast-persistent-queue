package com.btoddb.fastpersitentqueue;

import com.eaio.uuid.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * - append only writes (never do anything random)
 * - delete after all data has been successfully pop'ed
 * - read should be single threaded (only for replaying in some failure case)
 * - read use cases:
 *   - replay after shutdown or crash
 *   - replenish global memory queue if it was previously full
 *
 */
public class JournalFile {
    private static final Logger logger = LoggerFactory.getLogger(JournalFile.class);
    public static final int VERSION = 1;
    public static final int HEADER_SIZE = 20;

    private int version = VERSION;
    private File file;
    private UUID id;
    private RandomAccessFile writerFile;
    private RandomAccessFile readerFile;
    private ReentrantReadWriteLock writerLock = new ReentrantReadWriteLock();

    public JournalFile(File file) throws IOException {
        this.file = file;
    }

    public void initForReading() throws IOException {
        if (!file.exists()) {
            throw new FpqException("File does not exist with ID, " + id.toString());
        }

        try {
            readerFile = new RandomAccessFile(file, "r");
        }
        catch (FileNotFoundException e) {
            logger.error("exception while instantiating RandomAccessFile", e);
            throw e;
        }

        int tmp = readerFile.readInt();
        if (tmp != version) {
            throw new FpqException(String.format("version, %d, read from file, %s, does not match any expected versions", tmp, file.getCanonicalPath()));
        }
        id = Utils.readUuidFromFile(readerFile);
    }

    public void initForWriting(UUID id) throws IOException {
        if (file.exists()) {
            throw new FpqException("File already exists with ID, " + this.id.toString());
        }

        this.id = id;
        try {
            writerFile = new RandomAccessFile(file, "rw");
        }
        catch (FileNotFoundException e) {
            logger.error("exception while instantiating RandomAccessFile", e);
            throw e;
        }

        writerFile.writeInt(version);
        Utils.writeUuidToFile(writerFile, id);
    }

    public FpqEntry append(FpqEntry entry) throws IOException {
        Collection<FpqEntry> entries = append(Collections.singleton(entry));
        return entries.iterator().next();
    }

    public Collection<FpqEntry> append(Collection<FpqEntry> entries) throws IOException {
        writerLock.writeLock().lock();
        try {
            for (FpqEntry entry : entries) {
                switch (getVersion()) {
                    case 1:
                        entry.writeToJournal(writerFile);
                        break;
                    default:
                        Utils.logAndThrow(logger, String.format("invalid version (%d) found, cannot continue", getVersion()));
                }
            }

            // fsync will be called periodically in a separate thread
        }
        finally {
            writerLock.writeLock().unlock();
        }

        return entries;
    }
    
    public void forceFlush() throws IOException {
        if (null != writerFile) {
            writerFile.getChannel().force(true);
        }
    }

    public void close() throws IOException {
        // do flush otherwise channel.isOpen will report open, even after close
        forceFlush();
        if (null != readerFile) {
            readerFile.getChannel().force(true);
        }

        if (null != writerFile) {
            writerFile.close();
        }
        if (null != readerFile) {
            readerFile.close();
        }
    }

    public FpqEntry readNextEntry() throws IOException {
        FpqEntry entry = new FpqEntry();
        entry.setJournalId(id);
        entry.readFromDisk(readerFile);
        if (null != entry.getData()) {
            return entry;
        }
        else {
            return null;
        }
    }

    public long getReaderFilePosition() throws IOException {
        return readerFile.getFilePointer();
    }

    public long getWriterFilePosition() throws IOException {
        return writerFile.getFilePointer();
    }

    public File getFile() {
        return file;
    }

    // junit testing only
    RandomAccessFile getRandomAccessWriterFile() {
        return writerFile;
    }

    // junit testing only
    RandomAccessFile getRandomAccessReaderFile() {
        return readerFile;
    }

    public int getVersion() {
        return version;
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }
}
