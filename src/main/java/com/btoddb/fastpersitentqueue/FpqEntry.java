package com.btoddb.fastpersitentqueue;


import com.eaio.uuid.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;


/**
 *
 */
public class FpqEntry {
    private static final Logger logger = LoggerFactory.getLogger(FpqEntry.class);

    public static final UUID EMPTY_JOURNAL_ID = new UUID(0L, 0L);

    private long id;
    private byte[] data;
    private UUID journalId = EMPTY_JOURNAL_ID;

    public FpqEntry() {
    }

    public FpqEntry(long id, byte[] data) {
        this.id = id;
        this.data = data;
    }

    public long getMemorySize() {
        return 8 + // id length = long
                4 + // data length = integer
                24 + // UUID
                (null != data ? data.length : 0);
    }

    public void writeToJournal(RandomAccessFile raFile) throws IOException {
        Utils.writeLong(raFile, id);
        writeData(raFile);
    }

    public void readFromJournal(RandomAccessFile raFile) throws IOException {
        try {
            id = Utils.readLong(raFile);
            readData(raFile);
        }
        catch (EOFException e) {
            // ignore
        }
    }

    public void writeToPaging(RandomAccessFile raFile) throws IOException {
        Utils.writeLong(raFile, id);
        Utils.writeUuidToFile(raFile, journalId);
        writeData(raFile);
    }

    public void readFromPaging(RandomAccessFile raFile) throws IOException {
        id = Utils.readLong(raFile);
        journalId = Utils.readUuidFromFile(raFile);
        readData(raFile);
    }

    private void writeData(RandomAccessFile raFile) throws IOException {
        Utils.writeInt(raFile, getData().length);
        raFile.write(getData());
    }

    private void readData(RandomAccessFile raFile) throws IOException {
        int entryLength = Utils.readInt(raFile);
        data = new byte[entryLength];
        int readLength = raFile.read(data);
        if (readLength != data.length) {
            Utils.logAndThrow(logger, String.format("FPQ entry length (%s) could not be satisfied - file may be corrupted or code is out of sync with file version", entryLength));
        }
    }

    public byte[] getData() {
        return data;
    }

    public UUID getJournalId() {
        return journalId;
    }

    public void setJournalId(UUID journalId) {
        this.journalId = journalId;
    }

    public long getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FpqEntry fpqEntry = (FpqEntry) o;

        if (id != fpqEntry.id) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (id ^ (id >>> 32));
    }
}
