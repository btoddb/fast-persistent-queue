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

    private final Fpq queue;
    private final long durationOfTest;
    private final int entrySize;
    private final int batchSize;

    private boolean finished = false;
    private boolean success = false;
    int numberOfEntries = 0;

    public SpeedPushWorker(Fpq queue, long durationOfTest, int entrySize, int batchSize) {
        this.queue = queue;
        this.entrySize = entrySize;
        this.durationOfTest = durationOfTest;
        this.batchSize = batchSize;
    }

    @Override
    public void run() {
        try {
            long start = System.currentTimeMillis();
            long end = start + durationOfTest * 1000;
            FpqContext context = queue.createContext();
            while (System.currentTimeMillis() < end) {
                queue.push(context, new byte[entrySize]);
                numberOfEntries++;

                if (context.size() >= batchSize) {
                    queue.commit(context);
                }
            }
            if (0 < context.size()) {
                queue.commit(context);
            }
            success = true;
        }
        catch (Throwable e) {
            logger.error("exception while appending to journal", e);
        }

        finished = true;
    }

    public boolean isSuccess() {
        return success;
    }

    public boolean isFinished() {
        return finished;
    }

    public long getDurationOfTest() {
        return durationOfTest;
    }

    public int getEntrySize() {
        return entrySize;
    }

    public int getNumberOfEntries() {
        return numberOfEntries;
    }
}
