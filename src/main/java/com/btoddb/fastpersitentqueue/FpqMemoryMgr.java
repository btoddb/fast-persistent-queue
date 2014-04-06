package com.btoddb.fastpersitentqueue;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;


/**
 * - has fixed size (in bytes) queue segments
 */
public class FpqMemoryMgr {
    private long maxSegmentSizeInBytes;
    private LinkedList<MemorySegment> segments = new LinkedList<MemorySegment>();

    private AtomicLong numberOfEntries = new AtomicLong();

    public void init() {
        // TODO:BTB - reload any segments flushed to disk
        createNewSegment();
    }

    public void push(FpqEntry fpqEntry) {
        push(Collections.singleton(fpqEntry));
    }

    public void push(Collection<FpqEntry> events) {
        // - if enough free size to handle batch, then push events onto current segments
        // - if not, then create new segments and push there
        //   - if too many queues already, then flush newewst one we are not pushing to and load it later
        //

        while (!segments.peekLast().push(events)) {
            createNewSegment();
        }

        numberOfEntries.addAndGet(events.size());
    }

    private void createNewSegment() {
        MemorySegment seg = new MemorySegment();
        seg.setMaxSizeInBytes(maxSegmentSizeInBytes);
        segments.add(seg);
    }

    public Collection<FpqEntry> pop(int batchSize) {
        // TODO:BTB - manage reading new queue segment from disk if exists

        // - pop at most batchSize events from queue - do not wait to reach batchSize
        //   - if queue empty, do not wait, return empty list immediately

        // find the memory segment we need and reserve our entries
        MemorySegment chosenSegment = null;
        synchronized (segments) {
            Iterator<MemorySegment> iter = segments.iterator();
            while (iter.hasNext()) {
                MemorySegment seg = iter.next();
                long available = seg.getNumberOfAvailableEntries();
                if (0 < available) {
                    chosenSegment = seg;
                    seg.decrementAvailable(batchSize <= available ? batchSize : available);
                    break;
                }
            }
        }

        // if didn't find anything, return null
        if (null == chosenSegment) {
//            loadFromDiskIfAvailable();
            return null;
        }

        Collection<FpqEntry> entries = chosenSegment.pop(batchSize);
        numberOfEntries.addAndGet(-entries.size());

        if (chosenSegment.isAvailableForCleanup() && 0 == chosenSegment.getNumberOfAvailableEntries()) {
            synchronized (segments) {
                segments.remove(chosenSegment);
            }
        }

        return entries;
    }

    public long size() {
        return numberOfEntries.get();
    }

    Collection<MemorySegment> getSegments() {
        return segments;
    }

    public long getMaxSegmentSizeInBytes() {
        return maxSegmentSizeInBytes;
    }

    public void setMaxSegmentSizeInBytes(long maxSegmentSizeInBytes) {
        this.maxSegmentSizeInBytes = maxSegmentSizeInBytes;
    }

    public long getNumberOfEntries() {
        return numberOfEntries.get();
    }
}
