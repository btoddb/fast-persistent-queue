package com.btoddb.fastpersitentqueue.eventbus.plunkers;

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

import com.btoddb.fastpersitentqueue.Fpq;
import com.btoddb.fastpersitentqueue.eventbus.Config;
import com.btoddb.fastpersitentqueue.eventbus.EventBusComponentBaseImpl;
import com.btoddb.fastpersitentqueue.eventbus.FpqEvent;
import com.btoddb.fastpersitentqueue.eventbus.FpqPlunker;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * Sends the collection of events to an Flume AvroSource
 */
public class FlumeAvroPlunker extends EventBusComponentBaseImpl implements FpqPlunker {
    private AvroClientFactoryImpl clientFactory;


    /**
     * Happens inside a transaction.
     *
     * <p/>Any exceptions escaping this method will cauase a rollback
     *
     * @return true to commit TX, false to rollback
     */
    @Override
    public boolean handle(Collection<FpqEvent> events) throws Exception {
        List<Event> flumeEventList = new ArrayList<Event>(events.size());

        // convert FPQ events to flume events
        for (FpqEvent event : events) {
            flumeEventList.add(EventBuilder.withBody(event.getBody(), event.getHeaders()));
        }

        clientFactory.getInstanceAndSend(flumeEventList);

        return true;
    }

    @Override
    public void init(Config config) {
        super.init(config);
        clientFactory = new AvroClientFactoryImpl(new String[] {("localhost:4141")}, 1, 100, false, 120);
    }

    @Override
    public void shutdown() {
        clientFactory.shutdown();
    }
}
