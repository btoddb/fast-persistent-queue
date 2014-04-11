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

    private byte[] data;
    private UUID journalId = EMPTY_JOURNAL_ID;

    public FpqEntry() {
    }

    public FpqEntry(byte[] data) {
        this.data = data;
    }

    public long getMemorySize() {
        return 4 + // data length integer
                24 + // UUID
                (null != data ? data.length : 0);
    }

    public void writeToJournal(RandomAccessFile raFile) throws IOException {
        raFile.writeInt(getData().length);
        raFile.write(getData());
    }

    public void writeToPaging(RandomAccessFile raFile) throws IOException {
        Utils.writeUuidToFile(raFile, journalId);
        raFile.writeInt(getData().length);
        raFile.write(getData());
    }

    public void readFromJournal(RandomAccessFile raFile) throws IOException {
        try {
            readData(raFile);
        }
        catch (EOFException e) {
            // ignore
        }
    }

    public void readFromPaging(RandomAccessFile raFile) throws IOException {
        journalId = Utils.readUuidFromFile(raFile);
        readData(raFile);
    }

    private void readData(RandomAccessFile raFile) throws IOException {
        int entryLength = raFile.readInt();
        data = new byte[entryLength];
        int readLength = raFile.read(data);
        if (readLength != data.length) {
            Utils.logAndThrow(logger, String.format("FPQ entry length (%s) could not be satisfied - file may be corrupted or code is out of sync with file version", entryLength));
        }
    }

    public byte[] getData() {
        return data;
    }

//    public void setData(byte[] data) {
//        this.data = data;
//    }

    public UUID getJournalId() {
        return journalId;
    }

    public void setJournalId(UUID journalId) {
        this.journalId = journalId;
    }

}
