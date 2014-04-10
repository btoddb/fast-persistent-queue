package com.btoddb.fastpersitentqueue.speedtest;

import com.btoddb.fastpersitentqueue.Fpq;
import com.btoddb.fastpersitentqueue.FpqContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 */
public class SpeedPushWorker implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(SpeedPushWorker.class);

    private final Config config;
    private final Fpq queue;

    int numberOfEntries = 0;

    public SpeedPushWorker(Fpq queue, Config config) {
        this.queue = queue;
        this.config = config;
    }

    @Override
    public void run() {
        try {
            long start = System.currentTimeMillis();
            long end = start + config.getDurationOfTest() * 1000;
            FpqContext context = queue.createContext();
            while (System.currentTimeMillis() < end) {
                queue.push(context, new byte[config.getEntrySize()]);
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
            logger.error("exception while appending to journal", e);
        }
    }

    public int getNumberOfEntries() {
        return numberOfEntries;
    }
}
