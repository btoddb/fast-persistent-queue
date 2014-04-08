package com.btoddb.fastpersitentqueue;

import com.eaio.uuid.UUID;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


/**
 *
 */
public class MemorySegment {
    enum Status {
        READY, SAVING, LOADING, OFFLINE
    }

    public static final int VERSION = 1;

    private int version = VERSION;
    private UUID id;
    private long maxSizeInBytes = 10000000;

    private volatile Status status;
    private AtomicBoolean loaderTestAndSet = new AtomicBoolean();
    private AtomicBoolean removerTestAndSet = new AtomicBoolean();

    private ConcurrentLinkedQueue<FpqEntry> queue = new ConcurrentLinkedQueue<FpqEntry>();
    private AtomicLong sizeInBytes = new AtomicLong();
    private AtomicLong numberOfAvailableEntries = new AtomicLong();
    private volatile boolean pushingFinished;

    public boolean push(Collection<FpqEntry> events) {
        // - if enough free sizeInBytes to handle batch, then push events onto current queue
        // - if not, then throw exception

        long additionalSize = 0;
        for (FpqEntry entry : events) {
            additionalSize += entry.getMemorySize();
        }

        long newSize = sizeInBytes.addAndGet(additionalSize);
        numberOfAvailableEntries.addAndGet(events.size());

        synchronized (queue) {
            if (newSize > maxSizeInBytes) {
                pushingFinished = true;
                sizeInBytes.addAndGet(-additionalSize);
                numberOfAvailableEntries.addAndGet(-events.size());
                return false;
            }
        }

        queue.addAll(events);
        return true;
    }

    public Collection<FpqEntry> pop(int batchSize) {
        // - pop at most batchSize events from queue - do not wait to reach batchSize
        //   - if queue empty, do not wait, return empty list immediately

        ArrayList<FpqEntry> entryList = new ArrayList<FpqEntry>(batchSize);
        FpqEntry entry;
        while (entryList.size() < batchSize && null != (entry=queue.poll())) {
            entryList.add(entry);
        }
        sizeInBytes.addAndGet(-entryList.size());
        return entryList;
    }

    public void clearQueue() {
        queue.clear();
        sizeInBytes.set(0);
        numberOfAvailableEntries.set(0);
    }

    public void decrementAvailable(long count) {
        numberOfAvailableEntries.addAndGet(-count);
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

    public void writeToDisk(RandomAccessFile raFile) throws IOException {
        raFile.writeInt(getVersion());
        Utils.writeUuidToFile(raFile, id);
        raFile.writeLong(maxSizeInBytes);
        raFile.writeLong(getNumberOfAvailableEntries());
        raFile.writeLong(sizeInBytes.get());
        for (FpqEntry entry : getQueue()) {
            entry.writeToJournal(raFile);
        }
    }

    public void readFromDisk(RandomAccessFile raFile) throws IOException {
        readHeaderFromDisk(raFile);
        for ( int i=0;i < numberOfAvailableEntries.get();i++ ) {
            FpqEntry entry = new FpqEntry();
            entry.readFromDisk(raFile);
            entry.setJournalId(id);
            queue.add(entry);
        }
    }

    public void readHeaderFromDisk(RandomAccessFile raFile) throws IOException {
        version = raFile.readInt();
        id = Utils.readUuidFromFile(raFile);
        maxSizeInBytes = raFile.readLong();
        numberOfAvailableEntries.set(raFile.readLong());
        sizeInBytes.set(raFile.readLong());
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

    public ConcurrentLinkedQueue<FpqEntry> getQueue() {
        return queue;
    }
}
