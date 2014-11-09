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

import com.btoddb.fastpersitentqueue.exceptions.FpqMemorySegmentOffline;
import com.btoddb.fastpersitentqueue.exceptions.FpqPushFinished;
import com.btoddb.fastpersitentqueue.exceptions.FpqSegmentNotInReadyState;
import com.eaio.uuid.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 *
 */
public class MemorySegment implements Comparable<MemorySegment> {
    private static final Logger logger = LoggerFactory.getLogger(MemorySegment.class);

    enum Status {
        READY, SAVING, LOADING, OFFLINE, REMOVING
    }

    public static final int VERSION = 1;

    private int version = VERSION;
    private UUID id;
    private long maxSizeInBytes = 10000000;

    private volatile Status status;
    private volatile boolean pushingFinished;

    // these are so only one thread kicks of a load, remove, or page-to-disk
    private AtomicBoolean removerTestAndSet = new AtomicBoolean(true);
    private ReentrantReadWriteLock metaDataLock = new ReentrantReadWriteLock();

    private ConcurrentSkipListMap<Long, FpqEntry> queue = new ConcurrentSkipListMap<Long, FpqEntry>();
    private AtomicLong sizeInBytes = new AtomicLong();
    private AtomicLong numberOfEntries = new AtomicLong();
    private AtomicLong numberOfOnlineEntries = new AtomicLong();
    private AtomicLong totalEventsPushed = new AtomicLong();
    private AtomicLong totalEventsPopped = new AtomicLong();
    private long entryListOffsetOnDisk;

    // push is thread safe because ConcurrentSkipListMap says so
    // true = push success
    // false = not enough room and marked as finished
    public boolean push(Collection<FpqEntry> events, long spaceRequired) throws FpqPushFinished {
        // claim some room in this segment - if enough, then append entries and return
        // if not enough room in this segment, no attempt is made to push the partial batch and
        // this segment will be "push finished" regardless of how much room is available
        metaDataLock.readLock().lock();
        try {
            if (!isPushingFinished() && Status.READY == status && sizeInBytes.addAndGet(spaceRequired) <= maxSizeInBytes) {
                for (FpqEntry entry : events) {
                    queue.put(entry.getId(), entry);
                }
                totalEventsPushed.addAndGet(events.size());

                // this must be done last as it signals when events are ready to be popped from this segment
                numberOfEntries.addAndGet(events.size());
                numberOfOnlineEntries.addAndGet(events.size());
                return true;
            }
        }
        finally {
            metaDataLock.readLock().unlock();
        }

        // if got here, then not enough room to push *all* entries, so mark as finished
        // TODO:BTB - maybe should use sync section to reduce waiting on meta data write lock?
        metaDataLock.writeLock().lock();
        try {

            if (!isPushingFinished() && Status.READY == status) {
                // only one thread will set to true and throw exception.  the intent is that the segment
                // manager thread that catches exception should check for things like (should i page to disk, etc)
                setPushingFinished(true);
                throw new FpqPushFinished();
            }
        }
        finally {
            metaDataLock.writeLock().unlock();
        }

        return false;
    }

    public Collection<FpqEntry> pop(int batchSize) throws FpqSegmentNotInReadyState {
        metaDataLock.readLock().lock();
        try {
            // segment must be in READY state
            if (Status.READY != status) {
                throw new FpqSegmentNotInReadyState();
            }
            // find out if enough data remains in the segment.  if this segment has been removed, paged
            // to disk, etc, the adjustment will detect that too
            long remaining = numberOfOnlineEntries.addAndGet(-batchSize);
            long available = (long)batchSize + remaining;

            // if > 0 then we have a good segment, so pop from it!
            if (available > 0) {
                // adjust for partial batch
                if (remaining < 0) {
                    // don't just set to zero, many other threads are doing the same thing
                    numberOfOnlineEntries.addAndGet(-remaining);
                }
                else {
                    available = batchSize;
                }

                // we don't adjust number of available entries here.  it should have been adjusted
                // prior to popping to prevent unnecessary locking and still be thread-safe.  by the time
                // a thread gets here it should already know it had enough entries to pop 'batchSize' amount
                ArrayList<FpqEntry> entryList = new ArrayList<FpqEntry>(batchSize);
                long size = 0;
                Map.Entry<Long, FpqEntry> entry;
                while (entryList.size() < available && null != (entry=queue.pollFirstEntry())) {
                    entryList.add(entry.getValue());
                    size += entry.getValue().getMemorySize();
                }
                sizeInBytes.addAndGet(-size);
                numberOfEntries.addAndGet(-entryList.size());
                totalEventsPopped.addAndGet(entryList.size());
                return !entryList.isEmpty() ? entryList : null;
            }
            else {
                // means not even a single event can be popped (for whatever reason), so put back the way we found it
                numberOfOnlineEntries.addAndGet(batchSize);
                return null;
            }
        }
        finally {
            metaDataLock.readLock().unlock();
        }
    }

    public void clearQueue() {
        queue.clear();
    }

    public boolean removerTestAndSet() {
        return removerTestAndSet.compareAndSet(true, false);
    }

    public boolean isEntryQueued(FpqEntry entry) throws FpqMemorySegmentOffline {
        if (Status.OFFLINE != status) {
            return null != queue && queue.containsKey(entry.getId());
        }
        else {
            throw new FpqMemorySegmentOffline();
        }
    }

    public boolean shouldBeRemoved() {
        metaDataLock.writeLock().lock();
        try {
            // if pushing finished, then schedule segment to be removed.  seg.removerTestAndSet insures
            // that only one thread will schedule the remove - so make sure it is last in the conditionals
            if (isPushingFinished() && getNumberOfEntries() == 0 && removerTestAndSet()) {
                logger.debug("removing segment {} because its empty and pushing has finished", getId().toString());
                setStatus(MemorySegment.Status.REMOVING);
                return true;
            }
            return false;
        }
        finally {
            metaDataLock.writeLock().unlock();
        }
    }

    public void writeToDisk(RandomAccessFile raFile) throws IOException {
        writeHeaderToDisk(raFile);
        for (FpqEntry entry : getQueue().values()) {
            entry.writeToPaging(raFile);
        }
    }

    private void writeHeaderToDisk(RandomAccessFile raFile) throws IOException {
        Utils.writeInt(raFile, getVersion());
        Utils.writeUuidToFile(raFile, id);
        Utils.writeLong(raFile, maxSizeInBytes);
        Utils.writeLong(raFile, getNumberOfEntries());
        Utils.writeLong(raFile, sizeInBytes.get());
        Utils.writeLong(raFile, totalEventsPushed.get());
        Utils.writeLong(raFile, totalEventsPopped.get());
        entryListOffsetOnDisk = raFile.getFilePointer();
    }

    public void readFromPagingFile(RandomAccessFile raFile) throws IOException {
        readHeaderFromDisk(raFile);
        numberOfOnlineEntries.set(numberOfEntries.get());
        for ( int i=0;i < numberOfEntries.get();i++ ) {
            FpqEntry entry = new FpqEntry();
            entry.readFromPaging(raFile);
            queue.put(entry.getId(), entry);
        }
    }

    public void readHeaderFromDisk(RandomAccessFile raFile) throws IOException {
        version = raFile.readInt();
        id = Utils.readUuidFromFile(raFile);
        maxSizeInBytes = Utils.readLong(raFile);
        numberOfEntries.set(Utils.readLong(raFile));
        sizeInBytes.set(Utils.readLong(raFile));
        totalEventsPushed.set(Utils.readLong(raFile));
        totalEventsPopped.set(Utils.readLong(raFile));
        entryListOffsetOnDisk = raFile.getFilePointer();
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        metaDataLock.writeLock().lock();
        try {
            this.status = status;
        }
        finally {
            metaDataLock.writeLock().unlock();
        }
    }

    public int getVersion() {
        return version;
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public long getMaxSizeInBytes() {
        return maxSizeInBytes;
    }

    public void setMaxSizeInBytes(long maxSizeInBytes) {
        this.maxSizeInBytes = maxSizeInBytes;
    }

    public long getNumberOfEntries() {
        return numberOfEntries.get();
    }

    public long getNumberOfOnlineEntries() {
        return numberOfOnlineEntries.get();
    }

    public long getEntryListOffsetOnDisk() {
        return entryListOffsetOnDisk;
    }

    public boolean isPushingFinished() {
        return pushingFinished;
    }

    public void setPushingFinished(boolean pushingFinished) {
        metaDataLock.writeLock().lock();
        try {
            this.pushingFinished = pushingFinished;
        }
        finally {
            metaDataLock.writeLock().unlock();
        }
    }

    public ConcurrentSkipListMap<Long, FpqEntry> getQueue() {
        return queue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MemorySegment segment = (MemorySegment) o;

        if (id != null ? !id.equals(segment.id) : segment.id != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }

    @Override
    public int compareTo(MemorySegment o2) {
        // time based UUIDs
        return id.compareTo(o2.getId());
    }

    @Override
    public String toString() {
        return "MemorySegment{" +
                "id=" + id +
                ", status=" + status +
                ", pushingFinished=" + pushingFinished +
                ", sizeInBytes=" + sizeInBytes +
                ", numberOfEntries=" + numberOfEntries +
                ", numberOfOnlineEntries=" + numberOfOnlineEntries +
                ", totalEventsPushed=" + totalEventsPushed +
                ", totalEventsPopped=" + totalEventsPopped +
                ", entryListOffsetOnDisk=" + entryListOffsetOnDisk +
                ", queue size=" + (null != queue ? queue.size() : "null") +
                '}';
    }
}
