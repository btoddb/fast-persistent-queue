package com.btoddb.fastpersitentqueue;


import com.eaio.uuid.UUID;


/**
 *
 */
public class Entry {
    private int version = JournalFile.VERSION_1;
    private byte[] data;
    private UUID journalId;
    private long filePosition;

    public Entry() {
    }

    public Entry(byte[] data) {
        this.data = data;
    }

//    public Entry(int version, byte[] data, UUID journalId, long filePosition) {
//        this.version = version;
//        this.data = data;
//        this.journalId = journalId;
//        this.filePosition = filePosition;
//    }


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

    public long getFilePosition() {
        return filePosition;
    }

    public void setFilePosition(long filePosition) {
        this.filePosition = filePosition;
    }

    public UUID getJournalId() {
        return journalId;
    }

    public void setJournalId(UUID journalId) {
        this.journalId = journalId;
    }
}
