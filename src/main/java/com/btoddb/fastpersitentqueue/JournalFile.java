package com.btoddb.fastpersitentqueue;

import com.eaio.uuid.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
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
public class JournalFile implements Iterable<FpqEntry>, Iterator<FpqEntry> {
    private static final Logger logger = LoggerFactory.getLogger(JournalFile.class);
    public static final int VERSION = 1;
    public static final int HEADER_SIZE = 28;

    private int version = VERSION;
    private UUID id;
    private AtomicLong numberOfEntries = new AtomicLong();

    private File file;
    private RandomAccessFile raFile;
    private boolean writeMode;
    private ReentrantReadWriteLock writerLock = new ReentrantReadWriteLock();

    public JournalFile(File file) throws IOException {
        this.file = file;
    }

    public void initForReading() throws IOException {
        if (!file.exists()) {
            throw new FpqException("File does not exist with ID, " + id.toString());
        }

        try {
            raFile = new RandomAccessFile(file, "r");
            writeMode = false;
        }
        catch (FileNotFoundException e) {
            logger.error("exception while instantiating RandomAccessFile", e);
            throw e;
        }

        readHeader();
        if (0 == getNumberOfEntries()) {
            long filePos = raFile.getFilePointer();
            countEntriesInFile();
            raFile.seek(filePos);
        }
    }

    public void initForWriting(UUID id) throws IOException {
        if (file.exists()) {
            throw new FpqException("File already exists with ID, " + this.id.toString());
        }

        this.id = id;
        try {
            raFile = new RandomAccessFile(file, "rw");
            writeMode = true;
        }
        catch (FileNotFoundException e) {
            logger.error("exception while instantiating RandomAccessFile", e);
            throw e;
        }

        writeHeader();
    }

    private void readHeader() throws IOException {
        raFile.seek(0);
        int tmp = raFile.readInt();
        if (tmp != version) {
            throw new FpqException(String.format("invalid journal file version, %d.  file = %s", tmp, file.getCanonicalPath()));
        }
        id = Utils.readUuidFromFile(raFile);
        numberOfEntries.set(raFile.readLong());
    }

    private void countEntriesInFile() {
        Iterator<FpqEntry> iter = this;
        while (iter.hasNext()) {
            iter.next();
            numberOfEntries.incrementAndGet();
        }
    }

    public FpqEntry append(FpqEntry entry) throws IOException {
        append(Collections.singleton(entry));
        return entry;
    }

    public Collection<FpqEntry> append(Collection<FpqEntry> entries) throws IOException {
        writerLock.writeLock().lock();
        try {
            for (FpqEntry entry : entries) {
                switch (getVersion()) {
                    case 1:
                        entry.writeToJournal(raFile);
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

        numberOfEntries.incrementAndGet();
        return entries;
    }
    
    public void forceFlush() throws IOException {
        if (null != raFile) {
            raFile.getChannel().force(true);
        }
    }

    private void writeHeader() throws IOException {
        Utils.writeInt(raFile, version);
        Utils.writeUuidToFile(raFile, id);
        Utils.writeLong(raFile, numberOfEntries.get());
    }

    public void close() throws IOException {
        if (!isOpen()) {
            return;
        }

        if (writeMode) {
            raFile.seek(0);
            // this updates number of entries
            writeHeader();
        }

        // do flush otherwise channel.isOpen will report open, even after close
        forceFlush();
        raFile.close();
    }

    public FpqEntry readNextEntry() throws IOException {
        FpqEntry entry = new FpqEntry();
        entry.setJournalId(id);
        entry.readFromJournal(raFile);
        if (null != entry.getData()) {
            return entry;
        }
        else {
            return null;
        }
    }

    public long getFilePosition() throws IOException {
        return raFile.getFilePointer();
    }

    public File getFile() {
        return file;
    }

    // junit testing only
    RandomAccessFile getRandomAccessFile() {
        return raFile;
    }

    public int getVersion() {
        return version;
    }

    public UUID getId() {
        return id;
    }

    public long getNumberOfEntries() {
        return numberOfEntries.get();
    }

    @Override
    public Iterator<FpqEntry> iterator() {
        try {
            initForReading();
        }
        catch (IOException e) {
            logger.error("exception while setup of iterator", e);
        }
        return this;
    }

    @Override
    public boolean hasNext() {
        try {
            return raFile.getFilePointer() < raFile.length();
        }
        catch (IOException e) {
            Utils.logAndThrow(logger, "exception while determining EOF", e);
            return false;
        }
    }

    @Override
    public FpqEntry next() {
        try {
            return readNextEntry();
        }
        catch (IOException e) {
            Utils.logAndThrow(logger, "exception while reading next entry", e);
            return null;
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " does not implement 'remove'");
    }

    public boolean isOpen() {
        return null != raFile && raFile.getChannel().isOpen();
    }

    public boolean isWriteMode() {
        return writeMode;
    }
}
