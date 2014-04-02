package com.btoddb.fastpersitentqueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 *
 */
public class FlushRunner implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(FlushRunner.class);

    private final JournalFile journalFile;

    public FlushRunner(JournalFile journalFile) {
        this.journalFile = journalFile;
    }

    @Override
    public void run() {
        try {
            journalFile.forceFlush();
        }
        catch (IOException e) {
            logger.error("exception while fsync'ing", e);
        }
    }
}
