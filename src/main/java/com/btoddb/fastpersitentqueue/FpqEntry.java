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

    private byte[] data;
    private UUID journalId;

    public FpqEntry() {
    }

    public FpqEntry(byte[] data) {
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public UUID getJournalId() {
        return journalId;
    }

    public void setJournalId(UUID journalId) {
        this.journalId = journalId;
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

    public void writeToSpill(RandomAccessFile raFile) throws IOException {
        Utils.writeUuidToFile(raFile, journalId);
        raFile.writeInt(getData().length);
        raFile.write(getData());
    }

    public void readFromDisk(RandomAccessFile raFile) throws IOException {
        int entryLength;
        try {
            entryLength = raFile.readInt();
        }
        catch (EOFException e) {
            // ignore
            return;
        }

        data = new byte[entryLength];
        int readLength = raFile.read(data);
        if (readLength != data.length) {
            Utils.logAndThrow(logger, String.format("FPQ entry length (%s) could not be satisfied - file may be corrupted or code is out of sync with file version", entryLength));
        }
    }
}
