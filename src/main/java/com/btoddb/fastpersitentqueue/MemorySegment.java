package com.btoddb.fastpersitentqueue;

import com.eaio.uuid.UUID;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


/**
 *
 */
public class MemorySegment {
    enum Status {
        READY, SAVING, LOADING, OFFLINE, REMOVING
    }

    public static final int VERSION = 1;

    private int version = VERSION;
    private UUID id;
    private long maxSizeInBytes = 10000000;

    private volatile Status status;
    private volatile boolean pushingFinished;

    private AtomicBoolean loaderTestAndSet = new AtomicBoolean();
    private AtomicBoolean removerTestAndSet = new AtomicBoolean(true);

//    private ConcurrentLinkedQueue<FpqEntry> queue = new ConcurrentLinkedQueue<FpqEntry>();
    private ConcurrentSkipListMap<Long, FpqEntry> queue = new ConcurrentSkipListMap<Long, FpqEntry>();
    private AtomicLong sizeInBytes = new AtomicLong();
    private AtomicLong numberOfAvailableEntries = new AtomicLong();
    private AtomicLong totalEventsPushed = new AtomicLong();
    private AtomicLong totalEventsPopped = new AtomicLong();

    public void push(Collection<FpqEntry> events) {
        for (FpqEntry entry : events) {
            queue.put(entry.getId(), entry);
        }
        totalEventsPushed.addAndGet(events.size());

        // this must be done last as it signals when events are ready to be popped from this segment
        numberOfAvailableEntries.addAndGet(events.size());
    }

    public Collection<FpqEntry> pop(int batchSize) {
        // - pop at most batchSize events from queue - do not wait to reach batchSize
        //   - if queue empty, do not wait, return empty list immediately

        ArrayList<FpqEntry> entryList = new ArrayList<FpqEntry>(batchSize);
        long size = 0;
        Map.Entry<Long, FpqEntry> entry;
        while (entryList.size() < batchSize && null != (entry=queue.pollLastEntry())) {
            entryList.add(entry.getValue());
            size += entry.getValue().getMemorySize();
        }
        sizeInBytes.addAndGet(-size);
        totalEventsPopped.addAndGet(entryList.size());
        return entryList;
    }

    public void clearQueue() {
        queue.clear();
//        sizeInBytes.set(0);
//        numberOfAvailableEntries.set(0);
    }

    public long adjustSizeInBytes(long additionalSize) {
        return sizeInBytes.addAndGet(additionalSize);
    }

    public long decrementAvailable(long count) {
        return numberOfAvailableEntries.addAndGet(-count);
    }

    public long incrementAvailable(long count) {
        return numberOfAvailableEntries.addAndGet(count);
    }

    public boolean loaderTestAndSet() {
        return loaderTestAndSet.compareAndSet(true, false);
    }

    public boolean removerTestAndSet() {
        return removerTestAndSet.compareAndSet(true, false);
    }

    public void resetNeedLoadingTest() {
        loaderTestAndSet.set(true);
    }

    public boolean isEntryQueued(FpqEntry entry) {
        return queue.containsKey(entry.getId());
    }

    public void writeToDisk(RandomAccessFile raFile) throws IOException {
        Utils.writeInt(raFile, getVersion());
        Utils.writeUuidToFile(raFile, id);
        Utils.writeLong(raFile, maxSizeInBytes);
        Utils.writeLong(raFile, getNumberOfAvailableEntries());
        Utils.writeLong(raFile, sizeInBytes.get());
        Utils.writeLong(raFile, totalEventsPushed.get());
        Utils.writeLong(raFile, totalEventsPopped.get());
        for (FpqEntry entry : getQueue().values()) {
            entry.writeToPaging(raFile);
        }
    }

    public void readFromPagingFile(RandomAccessFile raFile) throws IOException {
        readHeaderFromDisk(raFile);
        for ( int i=0;i < numberOfAvailableEntries.get();i++ ) {
            FpqEntry entry = new FpqEntry();
            entry.readFromPaging(raFile);
            queue.put(entry.getId(), entry);
        }
    }

    public void readHeaderFromDisk(RandomAccessFile raFile) throws IOException {
        version = raFile.readInt();
        id = Utils.readUuidFromFile(raFile);
        maxSizeInBytes = raFile.readLong();
        numberOfAvailableEntries.set(raFile.readLong());
        sizeInBytes.set(raFile.readLong());
        totalEventsPushed.set(raFile.readLong());
        totalEventsPopped.set(raFile.readLong());
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public static int getVersion() {
        return VERSION;
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

    public long getNumberOfAvailableEntries() {
        return numberOfAvailableEntries.get();
    }

    public void setNumberOfAvailableEntries(long numberOfAvailableEntries) { this.numberOfAvailableEntries.set(numberOfAvailableEntries);}

    public boolean isPushingFinished() {
        return pushingFinished;
    }

    public void setPushingFinished(boolean pushingFinished) {
        this.pushingFinished = pushingFinished;
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
}
