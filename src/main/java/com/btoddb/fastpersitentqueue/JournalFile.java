package com.btoddb.fastpersitentqueue;

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

    public static final int VERSION_1 = 1;
    public static final int VERSION_1_OVERHEAD = 8;

    private File file;
    private RandomAccessFile writerFile;
    private RandomAccessFile readerFile;
    private ReentrantReadWriteLock writerLock = new ReentrantReadWriteLock();

    public JournalFile(File file) throws IOException {
        this.file = file;
        try {
            writerFile = new RandomAccessFile(file, "rw");
            readerFile = new RandomAccessFile(file, "r");
        }
        catch (FileNotFoundException e) {
            logger.error("exception while instantiating RandomAccessFile", e);
            throw e;
        }
    }

    public FpqEntry append(FpqEntry entry) throws IOException {
        Collection<FpqEntry> entries = append(Collections.singleton(entry));
        return entries.iterator().next();
    }

    public Collection<FpqEntry> append(Collection<FpqEntry> entries) throws IOException {
        writerLock.writeLock().lock();
        try {
            for (FpqEntry entry : entries) {
                entry.setFilePosition(writerFile.getFilePointer());
                switch (entry.getVersion()) {
                    case 1:
                        writeVersion1Entry(entry);
                        break;
                    default:
                        logAndThrow(String.format("invalid version (%d) found, cannot continue", entry.getVersion()));
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
        writerFile.getChannel().force(true);
    }

    public void close() throws IOException {
        // do flush otherwise channel.isOpen will report open, even after close
        forceFlush();
        readerFile.getChannel().force(true);

        writerFile.close();
        readerFile.close();
    }

    private void writeVersion1Entry(FpqEntry entry) throws IOException{
        writerFile.writeInt(entry.getVersion());
        writerFile.writeInt(entry.getData().length);
        writerFile.write(entry.getData());
    }

    public FpqEntry readNextEntry() throws IOException {
        FpqEntry entry = new FpqEntry();

        try {
            entry.setVersion(readerFile.readInt());
        }
        catch (EOFException e) {
            // no data - done
            return null;
        }

        switch (entry.getVersion()) {
            case 1:
                readVersion1Entry(entry);
                break;
            default:
                logAndThrow(String.format("invalid version (%d) found, cannot continue - file is corrupt or code is out of sync with file version", entry.getVersion()));
        }
        return entry;
    }

    private void logAndThrow(String msg) throws FpqException {
        logger.error(msg);
        throw new FpqException(msg);
    }

    private void readVersion1Entry(FpqEntry entry) throws IOException {
        int entryLength = readerFile.readInt();
        byte[] data = new byte[entryLength];
        int readLength = readerFile.read(data);
        if (readLength != data.length) {
            logAndThrow(String.format("entry version %s : entry length (%s) could not be satisfied - file may be corrupted or code is out of sync with file version", entry.getVersion(), entryLength));
        }
        entry.setData(data);
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
}
