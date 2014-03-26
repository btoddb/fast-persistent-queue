package com.btoddb.fastpersitentqueue;

import java.util.Collection;


/**
 * - has fixed size
 */
public class GlobalMemoryQueue {

    public void push(Collection<byte[]> events) {
        // - if enough free size to handle batch, then push events onto queue
        //   - if not, then throw exception
    }

    public Collection<byte[]> pop(int batchSize) {
        // - pop at most batchSize events from queue - do not wait to reach batchSize
        //   - if queue empty, do not wait, return empty list immediately


        return null;
    }
}
