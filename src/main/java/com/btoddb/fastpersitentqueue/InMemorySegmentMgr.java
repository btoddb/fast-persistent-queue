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
    private boolean shutdownInProgress;

    public void init() throws IOException {
        segmentSerializer.setDirectory(pagingDirectory);

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

        while (!segments.peekLast().push(events)) {
            segmentsLock.writeLock().lock();
            try {
                createNewSegment();
                if (numberOfActiveSegments.get() > maxNumberOfActiveSegments) {
                    // don't serialize the newest because we are "pushing" to it
                    Iterator<MemorySegment> iter = segments.descendingIterator();
                    iter.next(); // get past the newest
                    serializeToDisk(iter.next());
                }
            }
            finally {
                segmentsLock.writeLock().unlock();
            }
        }

        numberOfEntries.addAndGet(events.size());
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
        segmentsLock.readLock().lock();
        try {
            Iterator<MemorySegment> iter = segments.iterator();
            while (iter.hasNext()) {
                final MemorySegment seg = iter.next();
                if (MemorySegment.Status.READY != seg.getStatus()) {
                    if (seg.loaderTestAndSet()) {
                        kickOffLoad(seg);
                    }
                    continue;
                }

                long available = seg.getNumberOfAvailableEntries();
                if (0 < available) {
                    chosenSegment = seg;
                    seg.decrementAvailable(batchSize <= available ? batchSize : available);
                    break;
                }
                else if (seg.isPushingFinished() && seg.removerTestAndSet()) {
                    cleanupExecSrvc.submit(new Runnable() {
                        @Override
                        public void run() {
                            removeSegment(seg);
                        }
                    });
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

        Collection<FpqEntry> entries = chosenSegment.pop(batchSize);
        numberOfEntries.addAndGet(-entries.size());

        if (chosenSegment.isPushingFinished() && 0 == chosenSegment.getNumberOfAvailableEntries()) {
            segmentsLock.writeLock().lock();
            try {
                segments.remove(chosenSegment);
            }
            finally {
                segmentsLock.writeLock().unlock();
            }
        }

        return entries;
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
        // synchronization should already be done

        segment.setStatus(MemorySegment.Status.LOADING);

        serializerExecSrvc.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    segmentSerializer.loadFromDisk(segment);

                    // remove paging file before setting to READY in case something happens during remove
                    segmentSerializer.removePagingFile(segment);

                    segment.setPushingFinished(true);
                    segment.setStatus(MemorySegment.Status.READY);
                    numberOfActiveSegments.incrementAndGet();
                    segment.resetNeedLoadingTest();
                }
                catch (IOException e) {
                    logger.error("exception while loading memory segment, {}, from disk - discarding", segment.getId().toString(), e);
                    removeSegment(segment);
                }
            }
        });
    }

    private void removeSegment(MemorySegment segment) {
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
