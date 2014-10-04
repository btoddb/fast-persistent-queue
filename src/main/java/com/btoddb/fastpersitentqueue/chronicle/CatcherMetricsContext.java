package com.btoddb.fastpersitentqueue.chronicle;

import java.util.concurrent.TimeUnit;


/**
 * Metrics specific to catchers.
 */
public class CatcherMetricsContext {
//    private String componentId;

    private long batchStart;
    private long batchEnd;
    private int batchSize;

    public void startBatch(
//            String componentId
    ) {
//        this.componentId = componentId;
        batchStart = System.nanoTime();
    }

    public void endBatch() {
        batchEnd = System.nanoTime();
    }

    public long getPerEventDuration() {
        return TimeUnit.NANOSECONDS.toMicros((batchEnd - batchStart)/batchSize);
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

//    public String getComponentId() {
//        return componentId;
//    }
}
