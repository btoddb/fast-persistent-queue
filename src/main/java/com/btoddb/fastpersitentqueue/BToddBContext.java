package com.btoddb.fastpersitentqueue;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;


/**
 * !! NOT thread-safe.  use multiple contexts if multiple threads need to push/pop from queue.
 *
 */
public class BToddBContext {
    private final int maxTransactionSize;

    private Collection queue;
    private boolean pushing;
    private boolean popping;

    BToddBContext(int maxTransactionSize) {
        this.maxTransactionSize = maxTransactionSize;
    }

    @SuppressWarnings("unused")
    public void push(byte[] event) {
        push(Collections.singleton(event));
    }

    @SuppressWarnings("unused")
    public void push(Collection<byte[]> events) {
        if (!popping) {
            pushing = true;
        }
        else {
            throw new BToddBException("This context has already been used for 'popping'.  can't switch to pushing - create a new context");
        }

        if (null == queue) {
            queue = new LinkedList();
        }

        if (queue.size()+events.size() <= maxTransactionSize) {
            queue.addAll(events);
        }
        else {
            throw new BToddBException("pushing " + events.size() + " will exceed maximum transaction size of " + maxTransactionSize + " events");
        }
    }

    public void createPoppedEntries(Collection<Entry> entries) {
        if (!pushing) {
            popping = true;
        }
        else {
            throw new BToddBException("This context has already been used for 'pushing'.  can't switch to popping - create a new context");
        }

        this.queue = entries;
    }

    public void cleanup() {
        if (null != queue) {
            queue.clear();
            queue = null;
        }
        pushing = false;
        popping = false;
    }

    public int size() {
        return queue.size();
    }

    public boolean isPushing() {
        return pushing;
    }

    public boolean isPopping() {
        return popping;
    }

    public Collection getQueue() {
        if (null != queue) {
            return Collections.unmodifiableCollection(queue);
        }
        else {
            return null;
        }
    }
}
