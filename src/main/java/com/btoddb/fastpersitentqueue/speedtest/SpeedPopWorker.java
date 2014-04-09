package com.btoddb.fastpersitentqueue.speedtest;

import com.btoddb.fastpersitentqueue.Fpq;
import com.btoddb.fastpersitentqueue.FpqContext;
import com.btoddb.fastpersitentqueue.FpqEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;


/**
 * 
 */
public class SpeedPopWorker implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(SpeedPopWorker.class);

    private final Fpq queue;
    private final int batchSize;

    private boolean finished = false;
    private boolean success = false;
    int numberOfEntries = 0;
    private boolean stopWhenQueueEmpty = false;

    public SpeedPopWorker(Fpq queue, int batchSize) {
        this.queue = queue;
        this.batchSize = batchSize;
    }

    @Override
    public void run() {
        try {
            while (!stopWhenQueueEmpty || !queue.isEmpty()) {
                Thread.sleep(10);
                Collection<FpqEntry> entries;
                do {
                    FpqContext context = queue.createContext();
                    entries = queue.pop(context, batchSize);
                    if (null != entries && !entries.isEmpty()) {
                        queue.commit(context);
                        numberOfEntries += entries.size();
                        entries.clear();
                    }
                } while (null != entries && !entries.isEmpty());
            }
            success = true;
        }
        catch (Throwable e) {
            logger.error("exception while appending to journal", e);
            e.printStackTrace();
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
