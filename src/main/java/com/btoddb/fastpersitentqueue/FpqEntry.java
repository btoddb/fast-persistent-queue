package com.btoddb.fastpersitentqueue;


import com.eaio.uuid.UUID;

import java.io.IOException;
import java.io.RandomAccessFile;


/**
 *
 */
public class FpqEntry {
    public static final int VERSION = 1;

    private int version = VERSION;
    private byte[] data;
    private UUID journalId;
//    private long filePosition;

    public FpqEntry() {
    }

    public FpqEntry(byte[] data) {
        this.data = data;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

//    public long getFilePosition() {
//        return filePosition;
//    }
//
//    public void setFilePosition(long filePosition) {
//        this.filePosition = filePosition;
//    }

    public UUID getJournalId() {
        return journalId;
    }

    public void setJournalId(UUID journalId) {
        this.journalId = journalId;
    }

    public long getDiskSize() {
        return 4 + // version
                4 + // data length
                data.length;
    }
    public long getMemorySize() {
        return 4 + // version
                4 + // data length
                24 + // UUID
//                8 + // filePosition
                data.length;
    }

    public void writeToDisk(RandomAccessFile writerFile) throws IOException {
        writerFile.writeInt(getVersion());
        writerFile.writeInt(getData().length);
        writerFile.write(getData());
    }
}
