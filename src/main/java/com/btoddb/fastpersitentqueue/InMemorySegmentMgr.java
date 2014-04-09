package com.btoddb.fastpersitentqueue;

import com.eaio.uuid.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * - has fixed size (in bytes) queue segments
 */
public class InMemorySegmentMgr {
    private static final Logger logger = LoggerFactory.getLogger(InMemorySegmentMgr.class);

    private long maxSegmentSizeInBytes;
    private int maxNumberOfActiveSegments = 4;
    private AtomicInteger numberOfActiveSegments = new AtomicInteger();
    private volatile boolean shutdownInProgress;

    private ReentrantReadWriteLock segmentsLock = new ReentrantReadWriteLock();
    private LinkedList<MemorySegment> segments = new LinkedList<MemorySegment>();
    private AtomicLong numberOfEntries = new AtomicLong();
    private MemorySegmentSerializer segmentSerializer = new MemorySegmentSerializer();
    private File pagingDirectory;
    private int numberOfSerializerThreads = 2;
    private int numberOfCleanupThreads = 1;

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

    public void init() throws IOException {
        segmentSerializer.setDirectory(pagingDirectory);
        segmentSerializer.init();

        loadPagedSegments();

        createNewSegment();
    }

    private void loadPagedSegments() throws IOException {
        // read files then sort by their UUID name so they are in proper chronological order
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
        files = null;

        // reset
        numberOfActiveSegments.set(0);
        numberOfEntries.set(0);
        segments.clear();

        for (File f : sortedFiles) {
            if (numberOfActiveSegments.get() < maxNumberOfActiveSegments-1) {
                MemorySegment seg = segmentSerializer.loadFromDisk(f.getName());
                seg.setStatus(MemorySegment.Status.READY);
                seg.setPushingFinished(true);
                segments.add(seg);
                numberOfActiveSegments.incrementAndGet();
                numberOfEntries.addAndGet(seg.getNumberOfAvailableEntries());
            }
            else {
                MemorySegment seg = segmentSerializer.loadHeaderOnly(f.getName());
                seg.setStatus(MemorySegment.Status.OFFLINE);
                seg.setPushingFinished(true);
                segments.add(seg);
                numberOfEntries.addAndGet(seg.getNumberOfAvailableEntries());
            }
        }

        for (MemorySegment seg : segments) {
            if (seg.getStatus() == MemorySegment.Status.READY) {
                segmentSerializer.removePagingFile(seg);
            }
        }
    }

    public void push(FpqEntry fpqEntry) {
        push(Collections.singleton(fpqEntry));
    }

    public void push(Collection<FpqEntry> events) {
        // - if enough free size to handle batch, then push events onto current segments
        // - if not, then create new segments and push there
        //   - if too many queues already, then flush newewst one we are not pushing to and load it later
        //

        if (shutdownInProgress) {
            throw new FpqException("FPQ has been shutdown or is in progress, cannot push events");
        }

        long additionalSize = 0;
        for (FpqEntry entry : events) {
            additionalSize += entry.getMemorySize();
        }

        MemorySegment segment = segments.peekLast();

        // this must be done first as it signals if events can be pushed to this segment
        while (segment.adjustSizeInBytes(additionalSize) > maxSegmentSizeInBytes) {
            segment.adjustSizeInBytes(-additionalSize);
            segmentsLock.writeLock().lock();
            try {
                if (!segment.isPushingFinished()) {
                    segment.setPushingFinished(true);
                    createNewSegment();
                    if (numberOfActiveSegments.get() > maxNumberOfActiveSegments) {
                        pageSegmentToReduceActive();
                    }
                }
            }
            finally {
                segmentsLock.writeLock().unlock();
            }
            segment = segments.peekLast();
        }

        segment.push(events);

        numberOfEntries.addAndGet(events.size());
    }

    private void createNewSegment() {
        UUID newId = new UUID();

        MemorySegment seg = new MemorySegment();
        seg.setId(newId);
        seg.setMaxSizeInBytes(maxSegmentSizeInBytes);
        seg.setStatus(MemorySegment.Status.READY);

        segments.offer(seg);
        numberOfActiveSegments.incrementAndGet();
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

    public Collection<FpqEntry> pop(int batchSize) {
        // - pop at most batchSize events from queue - do not wait to reach batchSize
        //   - if queue empty, do not wait, return empty list immediately

        if (shutdownInProgress) {
            throw new FpqException("FPQ has been shutdown or is in progress, cannot pop events");
        }

        // find the memory segment we need and reserve our entries
        // will not use multiple segments to achieve 'batchSize'
        MemorySegment chosenSegment = null;
        int available = -1;
        segmentsLock.readLock().lock();
        try {
            for (final MemorySegment seg : segments) {
                // if segment is not READY then we can't pop from it.  however, if it is OFFLINE
                // we need to load it because it is next in FIFO order and *should* be popping from it
                synchronized (seg) {
                    if (MemorySegment.Status.READY != seg.getStatus()) {
                        if (MemorySegment.Status.OFFLINE == seg.getStatus() && seg.loaderTestAndSet()) {
                            kickOffLoad(seg);
                        }
                        continue;
                    }
                }

                // find out if enough data remains in the segment - uses Atomic so we can safely do this without
                // worrying about synchronization.
                long remaining = seg.decrementAvailable(batchSize);

                if (remaining+batchSize > 0) {
                    chosenSegment = seg;

                    // adjust for partial batch
                    if (remaining < 0) {
                        // don't just set to zero, could give improper indicator
                        seg.incrementAvailable(-remaining);
                        available = batchSize + (int)remaining;
                    }
                    else {
                        available = batchSize;
                    }
                    break;
                }
                // means not even a single event ready
                else {
                    // put back what we took as the segment may receive more events soon
                    seg.incrementAvailable(batchSize);

                    // if pushing finished, then schedule segment to be removed
                    // make sure the 'isPushingFinished' is always before the test-and-set
                    synchronized (seg) {
                        if (seg.isPushingFinished() && seg.getNumberOfAvailableEntries() == 0 && seg.removerTestAndSet()) {
                            logger.debug("removing segment {} because its empty and pushing has finished" +
                                                 "", seg.getId().toString());
                            seg.setStatus(MemorySegment.Status.REMOVING);
                            cleanupExecSrvc.submit(new Runnable() {
                                @Override
                                public void run() {
                                    removeSegment(seg);
                                }
                            });
                        }
                    }
                }
            }
        }
        finally {
            segmentsLock.readLock().unlock();
        }

        // if didn't find anything, return null
        if (null == chosenSegment) {
            return null;
        }

        Collection<FpqEntry> entries = chosenSegment.pop(available);
        numberOfEntries.addAndGet(-entries.size());

        return entries;
    }

    private void pageSegmentToReduceActive() {
        // synchronization should already be done

        // don't serialize the newest because we are "pushing" to it
        Iterator<MemorySegment> iter = segments.descendingIterator();
        iter.next(); // get past the newest
        serializeToDisk(iter.next());
    }

    private void serializeToDisk(final MemorySegment segment) {
        // synchronization should already be done

        segment.setStatus(MemorySegment.Status.SAVING);

        serializerExecSrvc.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    segmentSerializer.saveToDisk(segment);
                    segment.clearQueue();
                    segment.setStatus(MemorySegment.Status.OFFLINE);
                    segment.resetNeedLoadingTest();
                    numberOfActiveSegments.decrementAndGet();
                }
                catch (IOException e) {
                    logger.error("exception while saving memory segment, {}, to disk - discarding", segment.getId().toString(), e);
                    removeSegment(segment);
                }
            }
        });
    }

    private void kickOffLoad(final MemorySegment segment) {
        synchronized (segment) {
            segment.setStatus(MemorySegment.Status.LOADING);
        }

        serializerExecSrvc.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    synchronized (segment) {
                        segmentSerializer.loadFromDisk(segment);

                        // remove paging file before setting to READY in case something happens during remove
                        segmentSerializer.removePagingFile(segment);

                        segment.setPushingFinished(true);
                        segment.setStatus(MemorySegment.Status.READY);
                        numberOfActiveSegments.incrementAndGet();
                        segment.resetNeedLoadingTest();
                    }
                }
                catch (IOException e) {
                    logger.error("exception while loading memory segment, {}, from disk - discarding", segment.getId().toString(), e);
                    removeSegment(segment);
                }
            }
        });
    }

    private void removeSegment(MemorySegment segment) {
        logger.debug("removing segment {}", segment.getId().toString());
        numberOfActiveSegments.decrementAndGet();
        segmentsLock.writeLock().lock();
        try {
            segments.remove(segment);
        }
        finally {
            segmentsLock.writeLock().unlock();
        }
        segmentSerializer.removePagingFile(segment);
    }

    public long size() {
        return numberOfEntries.get();
    }

    public boolean isEmpty() {
        return 0 == size();
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
                serializeToDisk(segment);
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

    // can't set this, relies on being at least 4
    public int getMaxNumberOfActiveSegments() {
        return maxNumberOfActiveSegments;
    }

    public void setPagingDirectory(File pagingDirectory) {
        this.pagingDirectory = pagingDirectory;
    }

    public File getPagingDirectory() {
        return pagingDirectory;
    }
}
