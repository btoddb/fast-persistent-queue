package com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs;

import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * Maintains state for the {@link com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs.HdfsWriter}.  Keeps the
 * 'non-writer' logic out of the writer.
 */
public class WriterContext {
    private final HdfsWriter writer;
    private long createTime;
    private volatile long lastAccessTime;
    private volatile boolean active;

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public WriterContext(HdfsWriter writer) {
        this.writer = writer;
        this.createTime = System.currentTimeMillis();
        this.active = true;
    }

    public HdfsWriter getWriter() {
        return writer;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public void writeLock() {
        lock.writeLock().lock();
    }

    public void writeUnlock() {
        lock.writeLock().unlock();
    }

    public void readLock() {
        lock.readLock().lock();
    }

    public void readUnlock() {
        lock.readLock().unlock();
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }
}
