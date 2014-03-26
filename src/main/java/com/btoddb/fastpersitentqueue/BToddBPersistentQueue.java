package com.btoddb.fastpersitentqueue;

import java.util.Collection;
import java.util.Queue;


/**
 *
 */
public class BToddBPersistentQueue {

    public void push(BToddBContext context, Collection<byte[]> events) {
        // - write events to context.queue - done!  understood no persistence yet
    }

    public Queue<byte[]> pop(BToddBContext context) {
        // - move (up to) context.maxBatchSize events from globalMemoryQueue to context.queue
        // - do not wait for events
        // - return context.queue

        // assumptions
        // - can call 'pop' with same context over and over until context.queue has size context.maxBatchSize

        return context.getQueue();
    }

    public void commitForPop(BToddBContext context) {
        // - context.clearBatch
        // - if commit log file is no longer needed, remove in background work thread

    }

    public void commitForPush(BToddBContext context) {
        // - write events to commit log file (don't fsync)
        // - write events to globalMemoryQueue (for popping)
        // - roll persistent queue
        // - free context.queue

        // assumptions
        // - fsync thread will persist

    }

    public void rollbackForPush(BToddBContext context) {
        // - free context scoped memory queue
    }

    public void rollbackForPop(BToddBContext context) {
        // this one causes the problems with sync between memory and commit log files
        // - mv context.queue to front of globalMemoryQueue (can't move to end.  will screw up deleting of log files)
    }
}
