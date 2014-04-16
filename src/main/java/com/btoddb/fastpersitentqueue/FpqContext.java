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

    private Collection<FpqEntry> queue = new LinkedList<FpqEntry>();
    private boolean pushing;
    private boolean popping;
    private JmxMetrics jmxMetrics;

    FpqContext(AtomicLong idGen, int maxTransactionSize, JmxMetrics jmxMetrics) {
        this.idGen = idGen;
        this.maxTransactionSize = maxTransactionSize;
        this.jmxMetrics = jmxMetrics;
    }

    public void push(byte[] event) {
        push(Collections.singleton(event));
    }

    public void push(Collection<byte[]> entries) {
        if (!popping) {
            pushing = true;
        }
        else {
            throw new FpqException("This context has already been used for 'popping'.  can't switch to pushing - create a new context");
        }

        if (queue.size()+entries.size() <= maxTransactionSize) {
            for (byte[] entry : entries) {
                queue.add(new FpqEntry(idGen.incrementAndGet(), entry));
            }
        }
        else {
            throw new FpqException("pushing " + entries.size() + " will exceed maximum transaction size of " + maxTransactionSize + " events");
        }
        jmxMetrics.pushes.mark(entries.size());
    }

    public void createPoppedEntries(Collection<FpqEntry> entries) {
        if (!pushing) {
            popping = true;
        }
        else {
            throw new FpqException("This context has already been used for 'pushing'.  can't switch to popping - create a new context");
        }

        if (null == entries || entries.isEmpty()) {
            return;
        }

        // TODO:BTB - could use commons-collections to create a collection of collections
        this.queue.addAll(entries);
        jmxMetrics.pops.mark(entries.size());
    }

    public void cleanup() {
        if (null != queue) {
            // clearing the queue means that the push/pop methods must make a reference-copy of the entries
            queue.clear();
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
        if (!queue.isEmpty()) {
            return queue;
        }
        else{
            return null;
        }
    }
}
