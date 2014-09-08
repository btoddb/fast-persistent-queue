package com.btoddb.fastpersitentqueue;

/*
 * #%L
 * fast-persistent-queue
 * %%
 * Copyright (C) 2014 btoddb.com
 * %%
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * #L%
 */

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
