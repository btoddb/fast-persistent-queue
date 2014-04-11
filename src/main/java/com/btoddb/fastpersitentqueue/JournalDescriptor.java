package com.btoddb.fastpersitentqueue;

import com.eaio.uuid.UUID;

import java.io.IOException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;


/**
 *
 */
public class JournalDescriptor {
    private final UUID id;
    private final JournalFile file;
    private final ScheduledFuture future;
    private long startTime;

    private AtomicLong numberOfUnconsumedEntries = new AtomicLong();
    private volatile boolean writingFinished;

    public JournalDescriptor(JournalFile file) {
        this(file.getId(), file, null);
    }

    public JournalDescriptor(UUID id, JournalFile file, ScheduledFuture future) {
        this.id = id;
        this.file = file;
        this.future = future;
    }

    public UUID getId() {
        return id;
    }

    public JournalFile getFile() {
        return file;
    }

    public ScheduledFuture getFuture() {
        return future;
    }

//    public long incrementEntryCount(long size) {
//        return numberOfUnconsumedEntries.addAndGet(size);
//    }
//    public long decrementEntryCount(long size) {
//        return numberOfUnconsumedEntries.addAndGet(-size);
//    }

    public long adjustEntryCount(long delta) {
        return numberOfUnconsumedEntries.addAndGet(delta);
    }

    public boolean isWritingFinished() {
        return writingFinished;
    }

    public void setWritingFinished(boolean writingFinished) throws IOException {
//        length = getFile().getWriterFilePosition();
        this.writingFinished = writingFinished;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    long getNumberOfUnconsumedEntries() {
        return numberOfUnconsumedEntries.get();
    }

    public boolean isAnyWritesHappened() {
        return 0 < startTime;
    }

//    public long getLastPositionRead() {
//        return lastPositionRead;
//    }
//
//    public void setLastPositionRead(long lastPositionRead) {
//        this.lastPositionRead = lastPositionRead;
//    }

//    public long getLength() {
//        return length;
//    }
}
