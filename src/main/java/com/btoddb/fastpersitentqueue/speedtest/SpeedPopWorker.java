package com.btoddb.fastpersitentqueue.speedtest;

import com.btoddb.fastpersitentqueue.BToddBContext;
import com.btoddb.fastpersitentqueue.BToddBPersistentQueue;
import com.btoddb.fastpersitentqueue.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;


/**
 * 
 */
public class SpeedPopWorker implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(SpeedPopWorker.class);

    private final BToddBPersistentQueue queue;
    private final int batchSize;

    private boolean finished = false;
    private boolean success = false;
    int numberOfEntries = 0;
    private boolean stopWhenQueueEmpty = false;

    public SpeedPopWorker(BToddBPersistentQueue queue, int batchSize) {
        this.queue = queue;
        this.batchSize = batchSize;
    }

    @Override
    public void run() {
        try {
            BToddBContext context = queue.createContext();
            while (!stopWhenQueueEmpty || !queue.isEmpty()) {
                Thread.sleep(100);
                Collection<Entry> entries;
                do {
                    entries = queue.pop(context, batchSize);
                    if (!entries.isEmpty()) {
                        numberOfEntries += entries.size();
                        queue.commit(context);
                    }
                } while (!entries.isEmpty());
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

    public int getNumberOfEntries() {
        return numberOfEntries;
    }

    public void stopWhenQueueEmpty() {
        stopWhenQueueEmpty = true;
    }
}
