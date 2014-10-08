package com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs;

import java.util.concurrent.Future;


/**
 * Maintains state for the {@link com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs.HdfsWriter}.  Keeps the
 * 'non-writer' logic out of the writer.
 */
public class WriterContext {
    private Future<Void> idleFuture;
    private final HdfsWriter writer;

    public WriterContext(HdfsWriter writer) {
        this.writer = writer;
    }

    public Future<Void> getIdleFuture() {
        return idleFuture;
    }

    public void setIdleFuture(Future<Void> idleFuture) {
        this.idleFuture = idleFuture;
    }

    public HdfsWriter getWriter() {
        return writer;
    }
}
