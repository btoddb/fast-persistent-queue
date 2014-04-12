package com.btoddb.fastpersitentqueue;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;


/**
 * !! NOT thread-safe.  use multiple contexts if multiple threads need to push/pop from queue.
 *
 */
public class FpqContext {
    private final int maxTransactionSize;

    private Collection<FpqEntry> queue;
    private boolean pushing;
    private boolean popping;

    FpqContext(int maxTransactionSize) {
        this.maxTransactionSize = maxTransactionSize;
    }

    @SuppressWarnings("unused")
    public void push(FpqEntry event) {
        push(Collections.singleton(event));
    }

    @SuppressWarnings("unused")
    public void push(Collection<FpqEntry> events) {
        if (!popping) {
            pushing = true;
        }
        else {
            throw new FpqException("This context has already been used for 'popping'.  can't switch to pushing - create a new context");
        }

        if (null == queue) {
            queue = new LinkedList();
        }

        if (queue.size()+events.size() <= maxTransactionSize) {
            queue.addAll(events);
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
            // don't do queue.clear because more than likely the customer only copied reference to the list
            // not the individual items in the list
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
