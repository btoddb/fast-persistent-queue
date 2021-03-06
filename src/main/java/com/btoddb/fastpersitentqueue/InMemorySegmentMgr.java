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

import com.btoddb.fastpersitentqueue.exceptions.FpqException;
import com.btoddb.fastpersitentqueue.exceptions.FpqMemorySegmentOffline;
import com.btoddb.fastpersitentqueue.exceptions.FpqPushFinished;
import com.btoddb.fastpersitentqueue.exceptions.FpqSegmentNotInReadyState;
import com.eaio.uuid.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Manages the ordering and paging of memory segments.  Provides the push/pop entry points for
 * in-memory portion of FPQ.
 *
 */
public class InMemorySegmentMgr {
    private static final Logger logger = LoggerFactory.getLogger(InMemorySegmentMgr.class);

    // properties
    private long maxSegmentSizeInBytes;
    private int maxNumberOfActiveSegments = 4;
    private volatile boolean shutdownInProgress;
    private File pagingDirectory;
    private int numberOfSerializerThreads = 2;
    private int numberOfCleanupThreads = 1;

    // internals
    private final Object selectWhichSegmentLoadMonitor = new Object();
    private ConcurrentSkipListSet<MemorySegment> segments = new ConcurrentSkipListSet<MemorySegment>();
    private MemorySegmentSerializer segmentSerializer = new MemorySegmentSerializer();
    private JmxMetrics jmxMetrics;

    // statistics/reporting
    private AtomicInteger numberOfActiveSegments = new AtomicInteger();
    private AtomicLong numberOfEntries = new AtomicLong();
    private AtomicLong numberOfSwapOut = new AtomicLong();
    private AtomicLong numberOfSwapIn = new AtomicLong();


    private ExecutorService serializerExecSrvc = Executors.newFixedThreadPool(numberOfSerializerThreads,
                                                      new ThreadFactory() {
                                                          @Override
                                                          public Thread newThread(Runnable r) {
                                                              Thread t = new Thread(r);
                                                              t.setName("FPQ-"+MemorySegmentSerializer.class.getSimpleName());
                                                              return t;
                                                          }
                                                      });

    private ExecutorService cleanupExecSrvc = Executors.newFixedThreadPool(numberOfCleanupThreads,
                                                      new ThreadFactory() {
                                                          @Override
                                                          public Thread newThread(Runnable r) {
                                                              Thread t = new Thread(r);
                                                              t.setName("FPQ-Memory-Cleanup");
                                                              return t;
                                                          }
                                                      });

    public InMemorySegmentMgr(JmxMetrics jmxMetrics) {
        this.jmxMetrics = jmxMetrics;
    }

    public void init() throws IOException {
        if (0 == maxSegmentSizeInBytes) {
            throw new FpqException("property, maxSegmentSizeInBytes, must be greater than zero");
        }
        if (4 > maxNumberOfActiveSegments) {
            throw new FpqException("property, maxNumberOfActiveSegments, must be 4 or greater");
        }

        segmentSerializer.setDirectory(pagingDirectory);
        segmentSerializer.init();

        loadPagedSegments();
        createNewSegment();
    }

    private void loadPagedSegments() throws IOException {
        // read files and sort by their UUID name so they are in proper chronological order
        Collection<File> files = FileUtils.listFiles(pagingDirectory, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
        TreeSet<File> sortedFiles = new TreeSet<File>(new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                return new UUID(o1.getName()).compareTo(new UUID(o2.getName()));
            }
        });
        sortedFiles.addAll(files);

        // cleanup memory
        files.clear();

        assert 0 == numberOfActiveSegments.get();
        assert segments.isEmpty();

        segments.clear();

        for (File f : sortedFiles) {
            MemorySegment segment;
            // only load the segment's data if room in memory, otherwise just load its header
            if (numberOfActiveSegments.get() < maxNumberOfActiveSegments-1) {
                segment = segmentSerializer.loadFromDisk(f.getName());
                segment.setStatus(MemorySegment.Status.READY);
                segment.setPushingFinished(true);

                assert segment.getNumberOfOnlineEntries() > 0;
                assert !segment.getQueue().isEmpty();

                numberOfActiveSegments.incrementAndGet();
            }
            else {
                segment = segmentSerializer.loadHeaderOnly(f.getName());
                segment.setStatus(MemorySegment.Status.OFFLINE);
                segment.setPushingFinished(true);

            }

            assert segment.isPushingFinished();
            assert segment.getNumberOfEntries() > 0;
            assert segment.getEntryListOffsetOnDisk() > 0;

            segments.add(segment);
            numberOfEntries.addAndGet(segment.getNumberOfEntries());
        }

//        for (MemorySegment seg : segments) {
//            if (seg.getStatus() == MemorySegment.Status.READY) {
//                segmentSerializer.removePagingFile(seg);
//            }
//        }
    }

    public void push(FpqEntry fpqEntry) {
        push(Collections.singleton(fpqEntry));
    }

    /**
     * pick segment and push events.
     *
     * @param events
     */
    public void push(Collection<FpqEntry> events) {
        if (shutdownInProgress) {
            throw new FpqException("FPQ has been shutdown or is in progress, cannot push events");
        }

        // calculate memory used by the list of entries
        long spaceRequired = 0;
        for (FpqEntry entry : events) {
            spaceRequired += entry.getMemorySize();
        }

        if (spaceRequired > maxSegmentSizeInBytes) {
            throw new FpqException(String.format("the space required to push entries (%d) is greater than maximum segment size (%d) - increase segment size or reduce entry size", spaceRequired, maxSegmentSizeInBytes));
        }

        logger.debug("pushing {} event(s) totalling {} bytes", events.size(), spaceRequired);
        MemorySegment segment;
        while (true) {
            // grab the newest segment (last) and try to push to it.  we always push to the newest segment.
            // this is thread-safe because ConcurrentSkipListSet says so
            try {
                segment = segments.last();
            }
            catch (NoSuchElementException e) {
                logger.warn("no segments ready for pushing - not really a good thing");
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException e1) {
                    // ignore
                    Thread.interrupted();
                }
                continue;
            }

            // claim some room in this segment - if enough, then append entries and return
            // if not enough room in this segment, no attempt is made to push the partial batch and
            // this segment will be "push finished" regardless of how much room is available
            try {
                if (segment.push(events, spaceRequired)) {
                    logger.debug("pushed {} events to segment {}", events.size(), segment.getId());
                    numberOfEntries.addAndGet(events.size());
                    return;
                }
                else {
                    logger.debug("not enough room to push {} events to segment {} - skip to next segment (if there is one)", events.size(), segment.getId());
                }
            }
            // only one time per segment will this exception be thrown.  if caught, signals
            // this thread should check if needed to be paged out to disk
            catch (FpqPushFinished e) {
                createNewSegment();
                synchronized (numberOfActiveSegments) {
                    if (numberOfActiveSegments.get() > maxNumberOfActiveSegments) {
                        pageSegmentToDisk(segment);
                    }
                }
                logger.debug("creating new segment - now {} active segments", numberOfActiveSegments.get());
            }
        }
    }

    public FpqEntry pop() {
        Collection<FpqEntry> entries = pop(1);
        if (null != entries) {
            return entries.iterator().next();
        }
        else {
            return null;
        }
    }

    /**
     * pick segment and pop up to 'batchSize' events from it.
     *
     * @param batchSize
     * @return
     */
    public Collection<FpqEntry> pop(int batchSize) {
        if (shutdownInProgress) {
            throw new FpqException("FPQ has been shutdown or is in progress, cannot pop events");
        }

        logger.debug("popping up to {} events", batchSize);

        // find the memory segment we need and reserve our entries
        // will not use multiple segments to achieve 'batchSize'

        // we'll make only one pass through the segments.  if no entries available, or no segments ready
        // for popping (because could be OFFLINE, SAVING, etc), then null is returned (not empty collection)
        for (final MemorySegment seg : segments) {
            // guarantee no manipulation of segment while deciding from which segment to pop.
            // if segment is not READY then we can't pop from it.  however, if it is the first OFFLINE
            // segment in FIFO, then we *should* be popping from it.  but until it's loaded we skip it

            Collection<FpqEntry> entries;
            try {
                entries = seg.pop(batchSize);
            }
            catch (FpqSegmentNotInReadyState e) {
                logger.debug("segment, {}, is not in READY state ({}) - skipping", seg.getId(), seg.getStatus());
                continue;
            }

            if (null != entries) {
                logger.debug("popped {} entries from segment : {}", entries.size(), seg.toString());
                numberOfEntries.addAndGet(-entries.size());
                return entries;
            }

            // at this point no entries available for any thread to pop, so check if we can remove it
            // this call only returns true if segment is ready to be removed and this thread is the first to ask
            if (seg.shouldBeRemoved()) {
                logger.debug("scheduling {} for removal", seg.getId().toString());

                cleanupExecSrvc.submit(new Runnable() {
                    @Override
                    public void run() {
                        removeSegment(seg);
                    }
                });
            }
        }

        // if didn't find anything, return null
        return null;
    }

    private void createNewSegment() {
        UUID newId = new UUID();

        MemorySegment seg = new MemorySegment();
        seg.setId(newId);
        seg.setMaxSizeInBytes(maxSegmentSizeInBytes);
        seg.setStatus(MemorySegment.Status.READY);

        segments.add(seg);
        numberOfActiveSegments.incrementAndGet();
    }

    private void pageSegmentToDisk(final MemorySegment segment) {
        // thread-safe in the respect that only one thread should schedule this

        serializerExecSrvc.submit(new Runnable() {
            @Override
            public void run() {
                // make sure not determining which segment to page-in from disk
                logger.debug("set status to SAVING for segment {}", segment.getId());
                synchronized (selectWhichSegmentLoadMonitor) {
                    segment.setStatus(MemorySegment.Status.SAVING);
                }

                try {
                    logger.debug("serializing segment {} to page file", segment.getId());
                    if (null != jmxMetrics) {
                        jmxMetrics.pageOutSize.update(segment.getNumberOfEntries());
                    }

                    segmentSerializer.saveToDisk(segment);
                    segment.clearQueue();
                    segment.setStatus(MemorySegment.Status.OFFLINE);
                    numberOfActiveSegments.decrementAndGet();
                    numberOfSwapOut.incrementAndGet();
                }
                catch (IOException e) {
                    logger.error("exception while saving memory segment, {}, to disk - discarding segment (still have journals)", segment.getId().toString(), e);
                    removeSegment(segment);
                }
            }
        });
    }

    // this should be done in a thread and not in line with a customer call
    private void kickOffLoadIfNeeded() {
        MemorySegment tmp = null;
        boolean keepChecking = true;

        while (keepChecking) {
            keepChecking = false;
            synchronized (selectWhichSegmentLoadMonitor) {
                for (MemorySegment seg : segments) {
                    if (seg.getStatus() == MemorySegment.Status.OFFLINE) {
                        seg.setStatus(MemorySegment.Status.LOADING);
                        tmp = seg;
                        keepChecking = false;
                        break;
                    }
                    // if any segment is SAVING, then we want to keep checking if haven't found an OFFLINE
                    keepChecking = keepChecking || seg.getStatus() == MemorySegment.Status.SAVING;
                }
            }

            // let's sleep a bit while waiting on the save
            if (keepChecking) {
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException e) {
                    // set keepChecking to false so we stop looping and exit
                    keepChecking = false;
                }
            }
        }

        if (null == tmp) {
            return;
        }

        final MemorySegment segment = tmp;
        logger.debug("scheduling load of segment, {}", segment.getId());
        serializerExecSrvc.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    // segment is OFFLINE so no need for synchronization  during load
                    segmentSerializer.loadFromDisk(segment);
                    logger.debug("segment loaded = {}", segment.toString());

                    // remove paging file before setting to READY in case something happens during remove
                    segmentSerializer.removePagingFile(segment);

                    segment.setPushingFinished(true);
                    segment.setStatus(MemorySegment.Status.READY);
                    numberOfActiveSegments.incrementAndGet();

                    numberOfSwapIn.incrementAndGet();
                }
                catch (IOException e) {
                    logger.error("exception while loading memory segment, {}, from disk - discarding", segment.getId().toString(), e);
                    removeSegment(segment);
                }
            }
        });
    }

    // this should be done in a thread and not inline with a customer call
    private void removeSegment(MemorySegment segment) {
        logger.debug("removeSegment {}", segment.getId().toString());

        // this is thread-safe because ConcurrentSkipListSet says so
        if (!segments.remove(segment)) {
            logger.error("did not remove segment, {}, from set", segment.getId().toString());
        }

        numberOfActiveSegments.decrementAndGet();
        kickOffLoadIfNeeded();

        segmentSerializer.removePagingFile(segment);
    }

    public long size() {
        return numberOfEntries.get();
    }

    public boolean isEmpty() {
        return 0 == size();
    }

    public boolean isEntryQueued(FpqEntry entry) throws IOException {
        for (MemorySegment seg : segments) {
            try {
                if (seg.isEntryQueued(entry)) {
                    return true;
                }
            }
            catch (FpqMemorySegmentOffline e) {
                if(this.segmentSerializer.searchOffline(seg, entry)) {
                    return true;
                }
            }
        }
        return false;
    }


    public void shutdown() {
        shutdownInProgress = true;

        // wait until all segments are either READY or OFFLINE
        // then serialize the READYs
        for ( MemorySegment segment : segments ) {
            while (segment.getStatus() != MemorySegment.Status.READY
                    && segment.getStatus() != MemorySegment.Status.OFFLINE) {
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException e) {
                    // ignore
                    Thread.interrupted();
                }
            }

            if (segment.getStatus() == MemorySegment.Status.READY) {
                pageSegmentToDisk(segment);
            }
        }

        segments.clear();
        numberOfEntries.set(0);

        serializerExecSrvc.shutdown();
        try {
            serializerExecSrvc.awaitTermination(60, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            // ignore
            Thread.interrupted();
        }
        if (!serializerExecSrvc.isShutdown()) {
            serializerExecSrvc.shutdownNow();
        }

        cleanupExecSrvc.shutdown();
        try {
            cleanupExecSrvc.awaitTermination(60, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            // ignore
            Thread.interrupted();
        }
        if (!cleanupExecSrvc.isShutdown()) {
            cleanupExecSrvc.shutdownNow();
        }

        segmentSerializer.shutdown();
    }

    public int getNumberOfActiveSegments() {
        return numberOfActiveSegments.get();
    }

    public Collection<MemorySegment> getSegments() {
        return segments;
    }

    public void setMaxSegmentSizeInBytes(long maxSegmentSizeInBytes) {
        this.maxSegmentSizeInBytes = maxSegmentSizeInBytes;
    }

    public long getNumberOfEntries() {
        return numberOfEntries.get();
    }

    public void setPagingDirectory(File pagingDirectory) {
        this.pagingDirectory = pagingDirectory;
    }

    public long getNumberOfSwapOut() {
        return numberOfSwapOut.get();
    }

    public long getNumberOfSwapIn() {
        return numberOfSwapIn.get();
    }

    public int getNumberOfSerializerThreads() {
        return numberOfSerializerThreads;
    }

    public void setNumberOfSerializerThreads(int numberOfSerializerThreads) {
        this.numberOfSerializerThreads = numberOfSerializerThreads;
    }

    public int getNumberOfCleanupThreads() {
        return numberOfCleanupThreads;
    }

    public void setNumberOfCleanupThreads(int numberOfCleanupThreads) {
        this.numberOfCleanupThreads = numberOfCleanupThreads;
    }

    public int getMaxNumberOfActiveSegments() {
        return maxNumberOfActiveSegments;
    }

    public void setMaxNumberOfActiveSegments(int maxNumberOfActiveSegments) {
        this.maxNumberOfActiveSegments = maxNumberOfActiveSegments;
    }
}
