package com.btoddb.fastpersitentqueue.speedtest;

import com.btoddb.fastpersitentqueue.Fpq;
import com.btoddb.fastpersitentqueue.FpqContext;
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

    private final Fpq queue;
    private final Config config;
    private final AtomicLong sum;

    int numberOfEntries = 0;
    private boolean stopWhenQueueEmpty = false;

    public SpeedPopWorker(Fpq queue, Config config, AtomicLong sum) {
        this.queue = queue;
        this.config = config;
        this.sum = sum;
    }

    @Override
    public void run() {
        try {
            while (!stopWhenQueueEmpty || !queue.isEmpty()) {
                Thread.sleep(10);
                Collection<FpqEntry> entries;
                do {
                    FpqContext context = queue.createContext();
                    entries = queue.pop(context, config.getPopBatchSize());
                    if (null != entries && !entries.isEmpty()) {
                        numberOfEntries += entries.size();
                        queue.commit(context);
                        for (FpqEntry entry : entries) {
                            ByteBuffer bb = ByteBuffer.wrap(entry.getData());
                            sum.addAndGet(bb.getLong());
                        }
                        entries.clear();
                    }
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
