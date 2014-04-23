package com.btoddb.fastpersitentqueue.speedtest;

import com.btoddb.fastpersitentqueue.Fpq;
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
    private final Fpq fpq;
    private final AtomicLong counter;
    private final AtomicLong pushSum;

    int numberOfEntries = 0;

    public SpeedPushWorker(Fpq fpq, Config config, AtomicLong counter, AtomicLong pushSum) {
        this.fpq = fpq;
        this.config = config;
        this.counter = counter;
        this.pushSum = pushSum;
    }

    @Override
    public void run() {
        try {
            long start = System.currentTimeMillis();
            long end = start + config.getDurationOfTest() * 1000;
            while (System.currentTimeMillis() < end) {
                if (!fpq.isTransactionActive()) {
                    fpq.beginTransaction();
                }
                long x = counter.getAndIncrement();
                pushSum.addAndGet(x);
                fpq.push(createBody(x));
                numberOfEntries++;

                if (fpq.getCurrentTxSize() >= config.getPushBatchSize()) {
                    fpq.commit();
                }
            }
            if (0 < fpq.getCurrentTxSize()) {
                fpq.commit();
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
