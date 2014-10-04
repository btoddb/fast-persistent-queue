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


import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;


/**
 *
 */
public class CatcherMetrics extends ChronicleMetrics {

    private static ThreadLocal<CatcherMetricsContext> metrics = new ThreadLocal<CatcherMetricsContext>() {
        @Override
        protected CatcherMetricsContext initialValue() {
            return new CatcherMetricsContext();
        }
    };


    public CatcherMetrics() {
        super();
        reporter.start();
    }

    public void initialize(String componentId) {
//        registry.histogram(ClsMetrics.MESSAGE_SIZE, jmxKey);
//        catcherMetrics.meter(ClsMetrics.RECEIVE_RATE, jmxKey);
//        catcherMetrics.meter(ClsMetrics.UNAUTHORIZED, jmxKey);
        updateMetrics(componentId, null);
    }

    public void markBatchStart(String componentId) {
        metrics.get().startBatch();
    }

    public void markBatchEnd(String componentId) {
        metrics.get().endBatch();
        updateMetrics(componentId, metrics.get());
        metrics.remove();
    }

    private void updateMetrics(String componentId, CatcherMetricsContext context) {
        if (null != context) {
            registry.timer(name(componentId, "event-rate")).update(context.getPerEventDuration(), TimeUnit.MICROSECONDS);
            registry.histogram(name(componentId, "batch-size")).update(context.getBatchSize());
        }
        else {
            registry.timer(name(componentId, "event-rate"));
            registry.histogram(name(componentId, "batch-size"));
        }
    }

    public void setBatchSize(int batchSize) {
        metrics.get().setBatchSize(batchSize);
    }

    public void markStartRouting() {
//        catcherMetrics.get().startRouting();
    }

    public void markEndRouting() {
//        catcherMetrics.get().endRouting();
    }
}
