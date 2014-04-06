package com.btoddb.fastpersitentqueue;

import com.eaio.uuid.UUID;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;


/**
 *
 */
public class MemorySegment {
    private UUID id;
    private long maxSizeInBytes;

    private ConcurrentLinkedQueue<FpqEntry> queue = new ConcurrentLinkedQueue<FpqEntry>();
    private AtomicLong sizeInBytes = new AtomicLong();
    private AtomicLong numberOfAvailableEntries = new AtomicLong();
    private boolean availableForCleanup;

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
                availableForCleanup = true;
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

    public void decrementAvailable(long count) {
        numberOfAvailableEntries.addAndGet(-count);
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

    public boolean isAvailableForCleanup() {
        return availableForCleanup;
    }

    public void setAvailableForCleanup(boolean availableForCleanup) {
        this.availableForCleanup = availableForCleanup;
    }
}
