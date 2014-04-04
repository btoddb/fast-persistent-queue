package com.btoddb.fastpersitentqueue.speedtest;

import com.btoddb.fastpersitentqueue.BToddBPersistentQueue;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;


/**
 *
 */
public class SpeedTest {

    public static void main(String[] args) throws IOException, InterruptedException {
        File theDir = new File("speed"+ UUID.randomUUID().toString());
        FileUtils.forceMkdir(theDir);

        BToddBPersistentQueue queue = null;

        int maxTransactionSize = 2000;
        int pushBatchSize = 1;
        int popBatchSize = 2000;

        int numberOfPushers = 4;
        int numberOfPoppers = 4;
        int durationOfTest = 120; // seconds
        int entrySize = 1000;

        int maxJournalFileSize = 10000000;
        int journalMaxDurationInMs = 20000;
        int flushPeriodInMs = 1000;
        int numberOfFlushWorkers = 4;


        try {
            queue = new BToddBPersistentQueue();
            queue.setMemoryQueueMaxSize(10000000);
            queue.setMaxTransactionSize(maxTransactionSize);
            queue.setJournalDirectory(theDir);
            queue.setNumberOfFlushWorkers(numberOfFlushWorkers);
            queue.setFlushPeriodInMs(flushPeriodInMs);
            queue.setMaxJournalFileSize(maxJournalFileSize);
            queue.setMaxJournalDurationInMs(journalMaxDurationInMs);
            queue.init();

            Set<SpeedPushWorker> pushWorkers = new HashSet<SpeedPushWorker>();
            for (int i=0;i < numberOfPushers;i++) {
                pushWorkers.add(new SpeedPushWorker(queue, durationOfTest, entrySize, pushBatchSize));
            }

            Set<SpeedPopWorker> popWorkers = new HashSet<SpeedPopWorker>();
            for (int i=0;i < numberOfPoppers;i++) {
                popWorkers.add(new SpeedPopWorker(queue, popBatchSize));
            }

            ExecutorService pusherExecSrvc = Executors.newFixedThreadPool(numberOfPushers+numberOfPoppers, new ThreadFactory() {
                @Override
                public Thread newThread(Runnable runnable) {
                    Thread t = new Thread(runnable);
                    t.setName(runnable.getClass().getSimpleName());
                    return t;
                }
            });

            ExecutorService popperExecSrvc = Executors.newFixedThreadPool(numberOfPushers+numberOfPoppers, new ThreadFactory() {
                @Override
                public Thread newThread(Runnable runnable) {
                    Thread t = new Thread(runnable);
                    t.setName(runnable.getClass().getSimpleName());
                    return t;
                }
            });

            long startPushing = System.currentTimeMillis();
            for (SpeedPushWorker sw : pushWorkers) {
                pusherExecSrvc.submit(sw);
            }

            long startPopping = System.currentTimeMillis();
            for (SpeedPopWorker sw : popWorkers) {
                popperExecSrvc.submit(sw);
            }

            pusherExecSrvc.shutdown();
            pusherExecSrvc.awaitTermination(durationOfTest*2, TimeUnit.SECONDS);
            long endPushing = System.currentTimeMillis();

            // tell poppers, all pushers are finished
            for (SpeedPopWorker sw : popWorkers) {
                sw.stopWhenQueueEmpty();
            }

            popperExecSrvc.shutdown();
            popperExecSrvc.awaitTermination(durationOfTest*2, TimeUnit.SECONDS);
            long endPopping = System.currentTimeMillis();

            long numberOfPushes = 0;
            for (SpeedPushWorker sw : pushWorkers) {
                numberOfPushes += sw.getNumberOfEntries();
            }

            long numberOfPops = 0;
            for (SpeedPopWorker sw : popWorkers) {
                numberOfPops += sw.getNumberOfEntries();
            }

            long pushDuration = endPushing-startPushing;
            long popDuration = endPopping-startPopping;

            System.out.println("push duration = " + pushDuration);
            System.out.println("pushed = " + numberOfPushes);
            System.out.println("push entries/sec = " + numberOfPushes/(pushDuration/1000f));
            System.out.println();
            System.out.println("pop duration = " + popDuration);
            System.out.println("popped = " + numberOfPops);
            System.out.println("pop entries/sec = " + numberOfPops/(popDuration/1000f));
            System.out.println();
            System.out.println("journals created = " + queue.getJournalsCreated());
            System.out.println("journals removed = " + queue.getJournalsRemoved());
        }
        finally {
            if (null != queue) {
                queue.shutdown();
            }
//            FileUtils.deleteDirectory(theDir);
        }
    }
}
