package com.btoddb.fastpersitentqueue.chronicle.plunkers;

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

import com.btoddb.fastpersitentqueue.Utils;
import com.btoddb.fastpersitentqueue.chronicle.Config;
import com.btoddb.fastpersitentqueue.chronicle.FpqEvent;
import com.btoddb.fastpersitentqueue.chronicle.TokenizedFilePath;
import com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs.FileUtils;
import com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs.HdfsWriterFacoryImpl;
import com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs.HdfsWriterFactory;
import com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs.WriterContext;
import com.btoddb.fastpersitentqueue.chronicle.serializers.FpqEventSerializer;
import com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs.HdfsWriter;
import com.btoddb.fastpersitentqueue.chronicle.serializers.JsonSerializerImpl;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 *
 */
public class HdfsPlunkerImpl extends PlunkerBaseImpl {
    private static final Logger logger = LoggerFactory.getLogger(HdfsPlunkerImpl.class);

    private FpqEventSerializer serializer;
    private String pathPattern;
    private String permNamePattern;
    private String openNamePattern;
    private long rollPeriod = 600; // seconds (10 minutes)
    private long idleTimeout = 60; // seconds (1 minute)
    private int maxOpenFiles = 100;
    private int numIdleTimeoutThreads = 2;
    private int numCloseThreads = 4;
    private long shutdownWaitTimeout = 60;


    private Cache<String, WriterContext> writerCache;
    private FileUtils fileUtils = new FileUtils();

    private TokenizedFilePath keyTokenizedFilePath; // this is purely for HdfsWriter lookups
    private TokenizedFilePath permTokenizedFilePath;
    private TokenizedFilePath openTokenizedFilePath;

    private ScheduledThreadPoolExecutor idleTimerExec;
    private ScheduledThreadPoolExecutor closeExec;

    private AtomicBoolean isShutdown = new AtomicBoolean(false);
    private ReentrantReadWriteLock canHandleRequests = new ReentrantReadWriteLock();
    private HdfsWriterFactory writerFactory;


    @Override
    public void init(Config config) throws Exception {
        super.init(config);

        if (null == this.serializer) {
            this.serializer = new JsonSerializerImpl(config);
        }
        if (null == this.writerFactory) {
            this.writerFactory = new HdfsWriterFacoryImpl(config, serializer);
        }

        createExecutors();
        createFilePatterns();
        createWriterCache();
    }

    /**
     * Handle processing/saving events to HDFS.
     *
     * @param events collection of events
     * @throws Exception
     */
    @Override
    protected void handleInternal(Collection<FpqEvent> events) throws Exception {
        canHandleRequests.readLock().lock();
        try {
            if (isShutdown.get()) {
                logger.warn("rejecting request - plunker has been shutdown");
                return;
            }

            for (FpqEvent event : events) {
                // TODO:BTB - need some locking here to make sure we get a writer?
                WriterContext context = retrieveWriter(event);
                context.readLock();
                try {
                    context.getWriter().write(event);
                    context.setLastAccessTime(System.currentTimeMillis());
                }
                finally {
                    context.readUnlock();
                }
            }
        }
        finally {
            canHandleRequests.readLock().unlock();
        }
    }

    // closing HDFS files is done on a thread because it can take some time
    // also, if the close operation throws an exception, we try again
    private void submitClose(final WriterContext context) {
        closeExec.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    // writer must handle thread-safe closing
                    context.getWriter().close();
                }
                catch (IOException e) {
                    logger.error("exception while closing HdfsWriter - retrying", e);
                    try {
                        Thread.sleep(1000);
                    }
                    catch (InterruptedException e1) {
                        // ignore
                        Thread.interrupted();
                    }
                    // wait one second then try again
                    closeExec.schedule(this, 1, TimeUnit.SECONDS);
                }
            }
        });
    }

    private WriterContext retrieveWriter(final FpqEvent event) {
        try {
            return writerCache.get(keyTokenizedFilePath.createFileName(event.getHeaders()), new Callable<WriterContext>() {
                @Override
                public WriterContext call() throws IOException {
                    String permFileName = permTokenizedFilePath.createFileName(event.getHeaders());
                    String openFileName = openTokenizedFilePath.createFileName(event.getHeaders());
                    HdfsWriter writer = writerFactory.createWriter(permFileName, openFileName);
                    writer.init(config);
                    return new WriterContext(writer);
                }
            });
        }
        catch (ExecutionException e) {
            Utils.logAndThrow(logger, "exception while trying to retrieve PrintWriter from cache", e);
            return null;
        }
    }

    @Override
    public void shutdown() {
        if (!isShutdown.compareAndSet(false, true)) {
            logger.error("shutdown already called - returning");
            return;
        }

        idleTimerExec.shutdown();

        canHandleRequests.writeLock().lock();
        try {
            closeWritersAndWait();
        }
        finally {
            canHandleRequests.writeLock().unlock();
        }
    }

    private void closeWritersAndWait() {
        if (null != writerCache) {
            for (WriterContext context : writerCache.asMap().values()) {
                submitClose(context);
            }
        }

        closeExec.shutdown();

        try {
            if (!closeExec.awaitTermination(shutdownWaitTimeout, TimeUnit.SECONDS)) {
                closeExec.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            logger.error("exception while waiting for HdfsWriters to clowe", e);
        }
    }

    void createFilePatterns() {
        keyTokenizedFilePath = new TokenizedFilePath(fileUtils.concatPath(pathPattern, permNamePattern));
        permTokenizedFilePath = new TokenizedFilePath(fileUtils.concatPath(pathPattern, fileUtils.insertTimestamp(permNamePattern)));
        openTokenizedFilePath = new TokenizedFilePath(fileUtils.concatPath(pathPattern, fileUtils.insertTimestamp(openNamePattern)));
    }

    private void createExecutors() {
        if (null == idleTimerExec) {
            idleTimerExec = new ScheduledThreadPoolExecutor(
                    numIdleTimeoutThreads,
                    new ThreadFactory() {
                        @Override
                        public Thread newThread(Runnable r) {
                            Thread t = new Thread(r);
                            t.setName("FPQ-HDFS-IdleTimeout");
                            return t;
                        }
                    }
            );
        }

        if (null == closeExec) {
            closeExec = new ScheduledThreadPoolExecutor(
                    numCloseThreads,
                    new ThreadFactory() {
                        @Override
                        public Thread newThread(Runnable r) {
                            Thread t = new Thread(r);
                            t.setName("FPQ-HDFS-Closer");
                            return t;
                        }
                    },
                    new ThreadPoolExecutor.CallerRunsPolicy()
            );
        }

        // start scheduled idle task that checks if time to close file because of roll period or idle timeout
        idleTimerExec.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    // if any writer has been idle for longer than the "idle timeout"
                    // or the roll period has been exceeded, then close the file.  it
                    // will be reopened as needed when another request comes
                    long rollPeriodCutoff = System.currentTimeMillis()-TimeUnit.SECONDS.toMillis(rollPeriod);
                    long lastAccessCutoff = System.currentTimeMillis()-TimeUnit.SECONDS.toMillis(idleTimeout);
                    for (String key : writerCache.asMap().keySet()) {
                        WriterContext context = writerCache.getIfPresent(key);
                        if (null != context && context.isActive()
                                && (rollPeriodCutoff > context.getCreateTime() || lastAccessCutoff > context.getLastAccessTime())) {
                            context.writeLock();
                            try {
                                // if current thread is the one that got the write lock while still active, then we
                                // make it inactive so no other thread will do the same and start close procedure
                                if (context.isActive()) {
                                    context.setActive(false);
                                    // invalidating the cache will cause the writer to be closed
                                    writerCache.invalidate(key);
                                }
                            }
                            finally {
                                context.writeUnlock();
                            }
                        }
                    }
                }
            },
            10, 10, TimeUnit.SECONDS); // check every 10 seconds if file needs closing
    }

    private void createWriterCache() {
        writerCache = CacheBuilder.newBuilder()
                .maximumSize(maxOpenFiles)
                .removalListener(new RemovalListener<String, WriterContext>() {
                    @Override
                    public void onRemoval(RemovalNotification<String, WriterContext> entry) {
                        submitClose(entry.getValue());
                    }
                })
                .build();
    }

    public String getPathPattern() {
        return pathPattern;
    }

    public void setPathPattern(String pathPattern) {
        this.pathPattern = pathPattern;
    }

    public String getPermNamePattern() {
        return permNamePattern;
    }

    public void setPermNamePattern(String permNamePattern) {
        this.permNamePattern = permNamePattern;
    }

    public String getOpenNamePattern() {
        return openNamePattern;
    }

    public void setOpenNamePattern(String openNamePattern) {
        this.openNamePattern = openNamePattern;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(int idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public int getMaxOpenFiles() {
        return maxOpenFiles;
    }

    public void setMaxOpenFiles(int maxOpenFiles) {
        this.maxOpenFiles = maxOpenFiles;
    }

    public FpqEventSerializer getSerializer() {
        return serializer;
    }

    public void setSerializer(FpqEventSerializer serializer) {
        this.serializer = serializer;
    }

    public long getRollPeriod() {
        return rollPeriod;
    }

    public void setRollPeriod(int rollPeriod) {
        this.rollPeriod = rollPeriod;
    }

    public HdfsWriterFactory getWriterFactory() {
        return writerFactory;
    }

    public void setWriterFactory(HdfsWriterFactory writerFactory) {
        this.writerFactory = writerFactory;
    }

    public void setRollTimeout(long rollTimeout) {
        this.rollPeriod = rollTimeout;
    }

    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public int getNumIdleTimeoutThreads() {
        return numIdleTimeoutThreads;
    }

    public void setNumIdleTimeoutThreads(int numIdleTimeoutThreads) {
        this.numIdleTimeoutThreads = numIdleTimeoutThreads;
    }

    public int getNumCloseThreads() {
        return numCloseThreads;
    }

    public void setNumCloseThreads(int numCloseThreads) {
        this.numCloseThreads = numCloseThreads;
    }

    ScheduledThreadPoolExecutor getIdleTimerExec() {
        return idleTimerExec;
    }

    void setIdleTimerExec(ScheduledThreadPoolExecutor idleTimerExec) {
        this.idleTimerExec = idleTimerExec;
    }

    ScheduledThreadPoolExecutor getCloseExec() {
        return closeExec;
    }

    void setCloseExec(ScheduledThreadPoolExecutor closeExec) {
        this.closeExec = closeExec;
    }

    TokenizedFilePath getPermTokenizedFilePath() {
        return permTokenizedFilePath;
    }

    TokenizedFilePath getOpenTokenizedFilePath() {
        return openTokenizedFilePath;
    }

    TokenizedFilePath getKeyTokenizedFilePath() {
        return keyTokenizedFilePath;
    }

    public long getShutdownWaitTimeout() {
        return shutdownWaitTimeout;
    }

    public void setShutdownWaitTimeout(long shutdownWaitTimeout) {
        this.shutdownWaitTimeout = shutdownWaitTimeout;
    }

    public Collection<WriterContext> getWriters() {
        return writerCache.asMap().values();
    }
}
