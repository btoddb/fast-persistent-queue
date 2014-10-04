package com.btoddb.fastpersitentqueue.chronicle;

import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;


/**
 * Metrics specific to catchers.
 */
public class RequestMetrics {
    private MetricRegistry registry;
    private String componentId;

    private long batchStart;
    private int batchSize;

    public void startBatch(MetricRegistry registry, String componentId) {
        this.registry = registry;
        this.componentId = componentId;

//        registry.histogram(ClsMetrics.MESSAGE_SIZE, jmxKey);
//        metrics.meter(ClsMetrics.RECEIVE_RATE, jmxKey);
//        metrics.meter(ClsMetrics.UNAUTHORIZED, jmxKey);
        registry.timer(name(componentId, "event-rate"));

        batchStart = System.nanoTime();
    }

    public void endBatch() {
        long batchEnd = System.nanoTime();
        long perEventDuration = TimeUnit.NANOSECONDS.toMicros((batchEnd - batchStart)/batchSize);

        registry.timer(name(componentId, "event-rate")).update(perEventDuration, TimeUnit.MICROSECONDS);
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
