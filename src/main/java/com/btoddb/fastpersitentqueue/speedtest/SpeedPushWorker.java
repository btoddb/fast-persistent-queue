package com.btoddb.fastpersitentqueue.speedtest;

import com.btoddb.fastpersitentqueue.Fpq;
import com.btoddb.fastpersitentqueue.FpqContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;


/**
 * 
 */
public class SpeedPushWorker implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(SpeedPushWorker.class);

    private final Config config;
    private final Fpq queue;
    private final AtomicLong counter;
    private final AtomicLong pushSum;

    int numberOfEntries = 0;

    public SpeedPushWorker(Fpq queue, Config config, AtomicLong counter, AtomicLong pushSum) {
        this.queue = queue;
        this.config = config;
        this.counter = counter;
        this.pushSum = pushSum;
    }

    @Override
    public void run() {
        try {
            long start = System.currentTimeMillis();
            long end = start + config.getDurationOfTest() * 1000;
            FpqContext context = queue.createContext();
            while (System.currentTimeMillis() < end) {
                long x = counter.getAndIncrement();
                pushSum.addAndGet(x);
                queue.push(context, createBody(x));
                numberOfEntries++;

                if (context.size() >= config.getPushBatchSize()) {
                    queue.commit(context);
                }
            }
            if (0 < context.size()) {
                queue.commit(context);
            }
        }
        catch (Throwable e) {
            logger.error("exception while pushing to FPQ", e);
        }
    }

    private byte[] createBody(long x) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[config.getEntrySize()]);
        bb.putLong(x);
        return bb.array();
    }

    public int getNumberOfEntries() {
        return numberOfEntries;
    }
}
