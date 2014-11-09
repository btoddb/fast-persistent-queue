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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;


/**
 *
 */
public class JmxMetrics {
    public static final String JMX_ROOT_NAME = "com.btoddb.fpq";

    private final MetricRegistry metricsRegistry = new MetricRegistry();

    private Meter pushes;
    private Meter pops;
    public Histogram pageOutSize;
    public Counter size;

    private final Fpq fpq;

    public JmxMetrics(Fpq fpq) {
        this.fpq = fpq;
    }

    public void init() {
        JmxReporter reporter = JmxReporter.forRegistry(metricsRegistry).inDomain(JMX_ROOT_NAME+"."+fpq.getQueueName()).build();
        reporter.start();

        pushes = metricsRegistry.meter(MetricRegistry.name("pushes"));
        pops = metricsRegistry.meter(MetricRegistry.name("pops"));
        pageOutSize = metricsRegistry.histogram("entriesInPageOut");
        size = metricsRegistry.counter("size");

        metricsRegistry.register(MetricRegistry.name("journalFilesReplayed"),
                                 new Gauge<Long>() {
                                     @Override
                                     public Long getValue() {
                                         return fpq.getJournalFilesReplayed();
                                     }
                                 }
        );
        metricsRegistry.register(MetricRegistry.name("entriesReplayed"),
                                 new Gauge<Long>() {
                                     @Override
                                     public Long getValue() {
                                         return fpq.getJournalEntriesReplayed();
                                     }
                                 }
        );
    }

    public long getPopCount() {
        return pops.getCount();
    }

    public void incrementPops(int count) {
        pops.mark(count);
        size.dec(count);
    }

    public long getPushCount() {
        return pushes.getCount();
    }

    public void incrementPushes(int count) {
        pushes.mark(count);
        size.inc(count);
    }
}
