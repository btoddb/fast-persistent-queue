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

import com.btoddb.fastpersitentqueue.chronicle.Config;
import com.btoddb.fastpersitentqueue.chronicle.FpqEvent;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * Sends the collection of events to an Flume AvroSource
 */
public class FlumeAvroPlunkerImpl extends PlunkerBaseImpl {
    private AvroClientFactoryImpl clientFactory;

    private String[] hosts;
    private int connectionsPerHost = 1;
    private int maxBatchSize = 100;
    private int reconnectPeriod = 120;



    @Override
    protected void handleInternal(Collection<FpqEvent> events) throws Exception {
        List<Event> flumeEventList = new ArrayList<Event>(events.size());

        // convert FPQ events to flume events
        for (FpqEvent event : events) {
            flumeEventList.add(EventBuilder.withBody(event.getBody(), event.getHeaders()));
        }

        clientFactory.getInstanceAndSend(flumeEventList);
    }

    @Override
    public void init(Config config) throws Exception {
        super.init(config);
        clientFactory = new AvroClientFactoryImpl(hosts, connectionsPerHost, maxBatchSize, false, reconnectPeriod);
    }

    @Override
    public void shutdown() {
        clientFactory.shutdown();
    }

    public String[] getHosts() {
        return hosts;
    }

    public void setHosts(String[] hosts) {
        this.hosts = hosts;
    }

    public int getConnectionsPerHost() {
        return connectionsPerHost;
    }

    public void setConnectionsPerHost(int connectionsPerHost) {
        this.connectionsPerHost = connectionsPerHost;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    public int getReconnectPeriod() {
        return reconnectPeriod;
    }

    public void setReconnectPeriod(int reconnectPeriod) {
        this.reconnectPeriod = reconnectPeriod;
    }
}
