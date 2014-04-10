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
    private final Config config;

    int numberOfEntries = 0;
    private boolean stopWhenQueueEmpty = false;

    public SpeedPopWorker(Fpq queue, Config config) {
        this.queue = queue;
        this.config = config;
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
                        entries.clear();
                    }
                } while (null != entries && !entries.isEmpty());
            }
        }
        catch (Throwable e) {
            logger.error("exception while appending to journal", e);
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
