package com.btoddb.fastpersitentqueue.speedtest;

import com.btoddb.fastpersitentqueue.BToddBContext;
import com.btoddb.fastpersitentqueue.BToddBPersistentQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 */
public class SpeedPushWorker implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(SpeedPushWorker.class);

    private final BToddBPersistentQueue queue;
    private final long durationOfTest;
    private final int entrySize;

    private boolean finished = false;
    private boolean success = false;
    int numberOfEntries = 0;

    public SpeedPushWorker(BToddBPersistentQueue queue, long durationOfTest, int entrySize) {
        this.queue = queue;
        this.entrySize = entrySize;
        this.durationOfTest = durationOfTest;
    }

    @Override
    public void run() {
        try {
            long start = System.currentTimeMillis();
            long end = start + durationOfTest * 1000;
            BToddBContext context = queue.createContext();
            while (System.currentTimeMillis() < end) {
                queue.push(context, new byte[entrySize]);
                numberOfEntries++;

                if (context.size() >= queue.getMaxTransactionSize()) {
                    queue.commit(context);
                }
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
