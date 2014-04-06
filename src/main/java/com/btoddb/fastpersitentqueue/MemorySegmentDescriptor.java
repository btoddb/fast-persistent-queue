package com.btoddb.fastpersitentqueue;

import com.eaio.uuid.UUID;

import java.util.concurrent.atomic.AtomicBoolean;


/**
 *
 */
public class MemorySegmentDescriptor {
    enum Status {
        READY, SAVING, LOADING, OFFLINE
    }

    private UUID id;
    private MemorySegment segment;
    private Status status;
    private AtomicBoolean needLoadingTest = new AtomicBoolean();

    public boolean needLoadingTest() {
        return needLoadingTest.compareAndSet(true, false);
    }
    public void resetNeedLoadingTest() {
        needLoadingTest.set(true);
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public MemorySegment getSegment() {
        return segment;
    }

    public void setSegment(MemorySegment segment) {
        this.segment = segment;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }
}
