package com.btoddb.fastpersitentqueue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;


/**
 * - has fixed size
 */
public class FpqMemoryMgr {
    private long maxSize;
    private ConcurrentLinkedQueue<FpqEntry> queue = new ConcurrentLinkedQueue<FpqEntry>();

    private AtomicLong size = new AtomicLong();

    public void push(Collection<FpqEntry> events) {
        // TODO:BTB - manage rolling and flushing queue to disk if filled because popping is too slow
        // - if enough free size to handle batch, then push events onto queue
        //   - if not, then throw exception

        long newSize = size.addAndGet(events.size());
        if (newSize > maxSize) {
            size.addAndGet(-events.size());
            throw new FpqException("pushing " + events.size() + " will exceed maximum queue size of " + maxSize + " events");
        }

        queue.addAll(events);
    }

    public Collection<FpqEntry> pop(int batchSize) {
        // TODO:BTB - manage reading new queue segment from disk if exists

        // - pop at most batchSize events from queue - do not wait to reach batchSize
        //   - if queue empty, do not wait, return empty list immediately

        ArrayList<FpqEntry> entryList = new ArrayList<FpqEntry>(batchSize);
        FpqEntry entry;
        while (entryList.size() < batchSize && null != (entry=queue.poll())) {
            entryList.add(entry);
        }
        size.addAndGet(-entryList.size());
        return entryList;
    }

    public long size() {
        return size.get();
    }

    public long getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(long maxSize) {
        this.maxSize = maxSize;
    }
}
