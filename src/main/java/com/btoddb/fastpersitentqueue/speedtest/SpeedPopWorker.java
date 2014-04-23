package com.btoddb.fastpersitentqueue.speedtest;

import com.btoddb.fastpersitentqueue.Fpq;
import com.btoddb.fastpersitentqueue.FpqEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;


/**
 * 
 */
public class SpeedPopWorker implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(SpeedPopWorker.class);

    private final Fpq fpq;
    private final Config config;
    private final AtomicLong sum;

    int numberOfEntries = 0;
    private boolean stopWhenQueueEmpty = false;

    public SpeedPopWorker(Fpq fpq, Config config, AtomicLong sum) {
        this.fpq = fpq;
        this.config = config;
        this.sum = sum;
    }

    @Override
    public void run() {
        try {
            while (!stopWhenQueueEmpty || !fpq.isEmpty()) {
                Thread.sleep(10);
                Collection<FpqEntry> entries;
                do {
                    fpq.beginTransaction();
                    entries = fpq.pop(config.getPopBatchSize());
                    if (null != entries && !entries.isEmpty()) {
                        numberOfEntries += entries.size();
                        for (FpqEntry entry : entries) {
                            ByteBuffer bb = ByteBuffer.wrap(entry.getData());
                            sum.addAndGet(bb.getLong());
                        }
                        entries.clear();
                    }
                    fpq.commit();
                } while (null != entries && !entries.isEmpty());
            }
        }
        catch (Throwable e) {
            logger.error("exception while popping from FPQ", e);
            e.printStackTrace();
        }
    }

    public int getNumberOfEntries() {
        return numberOfEntries;
    }

    public void stopWhenQueueEmpty() {
        stopWhenQueueEmpty = true;
    }
}
