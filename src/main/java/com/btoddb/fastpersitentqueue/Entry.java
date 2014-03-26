package com.btoddb.fastpersitentqueue;

/**
 *
 */
public class Entry {
    private int version;
    private byte[] data;
    private long filePosition;

    public Entry(int version, byte[] data, long filePosition) {
        this.version = version;
        this.data = data;
        this.filePosition = filePosition;
    }

    public Entry() {

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

    public long getFilePosition() {
        return filePosition;
    }

    public void setFilePosition(long filePosition) {
        this.filePosition = filePosition;
    }
}
