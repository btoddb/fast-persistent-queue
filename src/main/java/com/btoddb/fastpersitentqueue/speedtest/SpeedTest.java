package com.btoddb.fastpersitentqueue.speedtest;

import com.btoddb.fastpersitentqueue.JournalFileMgr;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;


/**
 *
 */
public class SpeedTest {

    public static void main(String[] args) throws IOException, InterruptedException {
        File theDir = new File("speed"+ UUID.randomUUID().toString());
        FileUtils.forceMkdir(theDir);

        JournalFileMgr mgr = null;


        int numberOfWorkers = 2;
        int durationOfTest = 10; // seconds
        int entrySize = 1000;

        int maxJournalFileSize = 10000000;
        int journalMaxDurationInMs = 20000;
        int flushPeriodInMs = 1000;
        int numberOfFlushWorkers = 4;


        try {
            mgr = new JournalFileMgr();
            mgr.setJournalDirName(theDir.getAbsolutePath());
            mgr.setNumberOfFlushWorkers(numberOfFlushWorkers);
            mgr.setFlushPeriodInMs(flushPeriodInMs);
            mgr.setMaxJournalFileSize(maxJournalFileSize);
            mgr.setMaxJournalDurationInMs(journalMaxDurationInMs);
            mgr.start();

            Set<SpeedWorker> workers = new HashSet<SpeedWorker>();
            for (int i=0;i < numberOfWorkers;i++) {
                workers.add( new SpeedWorker(mgr, durationOfTest, entrySize) );
            }


            long start = System.currentTimeMillis();
            long end = start + durationOfTest*1000;
            int numberOfEntries = 0;
            while (System.currentTimeMillis() < end) {
                mgr.append(new byte[entrySize]);
                numberOfEntries ++;
            }

            long duration = (System.currentTimeMillis()-start);
            System.out.println("duration = " + duration + " to write " + (numberOfEntries*entrySize) + " bytes");
            System.out.println("entries/sec = " + numberOfEntries/(duration/1000f));
        }
        finally {
            if (null != mgr) {
                mgr.shutdown();
            }
//            FileUtils.deleteDirectory(theDir);
        }
    }
}
