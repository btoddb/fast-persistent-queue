package com.btoddb.fastpersitentqueue;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;


/**
 * !! NOT thread-safe.  use multiple contexts if multiple threads need to push/pop from queue.
 *
 */
public class FpqContext {
    private final int maxTransactionSize;
    private final AtomicLong idGen;

    private Collection<FpqEntry> queue;
    private boolean pushing;
    private boolean popping;

    FpqContext(AtomicLong idGen, int maxTransactionSize) {
        this.idGen = idGen;
        this.maxTransactionSize = maxTransactionSize;
    }

    public void push(byte[] event) {
        push(Collections.singleton(event));
    }

    public void push(Collection<byte[]> events) {
        if (!popping) {
            pushing = true;
        }
        else {
            throw new FpqException("This context has already been used for 'popping'.  can't switch to pushing - create a new context");
        }

        if (null == queue) {
            queue = new LinkedList<FpqEntry>();
        }

        if (queue.size()+events.size() <= maxTransactionSize) {
            for (byte[] event : events) {
                queue.add(new FpqEntry(idGen.incrementAndGet(), event));
            }
        }
        else {
            throw new FpqException("pushing " + events.size() + " will exceed maximum transaction size of " + maxTransactionSize + " events");
        }
    }

    public void createPoppedEntries(Collection<FpqEntry> entries) {
        if (!pushing) {
            popping = true;
        }
        else {
            throw new FpqException("This context has already been used for 'pushing'.  can't switch to popping - create a new context");
        }

        this.queue = entries;
    }

    public void cleanup() {
        if (null != queue) {
            // clearing the queue means that the push method must make a reference-copy of the entries
            queue.clear();
            queue = null;
        }
        pushing = false;
        popping = false;
    }

    public int size() {
        return null != queue ? queue.size() : 0;
    }

    public boolean isPushing() {
        return pushing;
    }

    public boolean isPopping() {
        return popping;
    }

    public boolean isQueueEmpty() {
        return null == queue || queue.isEmpty();
    }

    public Collection getQueue() {
        return queue;
    }
}
