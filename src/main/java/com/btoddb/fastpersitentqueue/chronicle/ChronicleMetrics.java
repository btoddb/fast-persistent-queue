package com.btoddb.fastpersitentqueue.chronicle;

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

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;


/**
 *
 */
public class ChronicleMetrics {
    private MetricRegistry registry = new MetricRegistry();
    private JmxReporter reporter;

    private static ThreadLocal<RequestMetrics> metrics = new ThreadLocal<RequestMetrics>() {
        @Override
        protected RequestMetrics initialValue() {
            return new RequestMetrics();
        }
    };


    public ChronicleMetrics() {
        reporter = JmxReporter.forRegistry(registry).inDomain("fpq.chronicle").build();
        reporter.start();
    }

    public MetricRegistry getRegistry() {
        return registry;
    }

    public void markBatchStart(String componentId) {
        metrics.get().startBatch(registry, componentId);
    }

    public void markBatchEnd() {
        metrics.get().endBatch();
        metrics.remove();
    }

    public void setBatchSize(int batchSize) {
        metrics.get().setBatchSize(batchSize);
    }

    public void markStartRouting() {
//        metrics.get().startRouting();
    }

    public void markEndRouting() {
//        metrics.get().endRouting();
    }
}
