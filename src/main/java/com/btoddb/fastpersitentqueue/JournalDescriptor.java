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

//    private long lastPositionRead = -1;
    private AtomicLong numberOfUnconsumedEntries = new AtomicLong();
    private boolean writingFinished;
//    private long length;

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

    public long incrementEntryCount(int size) {
        return numberOfUnconsumedEntries.addAndGet(size);
    }
    public long decrementEntryCount(int size) {
        return numberOfUnconsumedEntries.addAndGet(-size);
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
