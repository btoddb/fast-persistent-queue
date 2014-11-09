package com.btoddb.fastpersitentqueue;

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

import com.btoddb.fastpersitentqueue.exceptions.FpqException;

import java.util.Collection;
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

    FpqContext(AtomicLong idGen, int maxTransactionSize) {
        this.idGen = idGen;
        this.maxTransactionSize = maxTransactionSize;
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

    public Collection<FpqEntry> getQueue() {
        if (!queue.isEmpty()) {
            return queue;
        }
        else{
            return null;
        }
    }
}
