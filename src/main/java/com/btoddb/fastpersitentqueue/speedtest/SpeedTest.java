package com.btoddb.fastpersitentqueue.speedtest;

import com.btoddb.fastpersitentqueue.Fpq;
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

    public static void main(String[] args) throws Exception {
        if (0 == args.length) {
            System.out.println();
            System.out.println("ERROR: must specify the config file path/name");
            System.out.println();
            System.exit(1);
        }

        Config config = new Config(args[0]);

        System.out.println(config.toString());

        File theDir = new File(config.getDirectory(), "speed-"+ UUID.randomUUID().toString());
        FileUtils.forceMkdir(theDir);

        Fpq queue = null;

        try {
            queue = new Fpq();
            queue.setMaxMemorySegmentSizeInBytes(config.getMaxMemorySegmentSizeInBytes());
            queue.setMaxTransactionSize(config.getMaxTransactionSize());
            queue.setJournalDirectory(new File(theDir, "journal"));
            queue.setPagingDirectory(new File(theDir, "paging"));
            queue.setNumberOfFlushWorkers(config.getNumberOfFlushWorkers());
            queue.setFlushPeriodInMs(config.getFlushPeriodInMs());
            queue.setMaxJournalFileSize(config.getMaxJournalFileSize());
            queue.setMaxJournalDurationInMs(config.getJournalMaxDurationInMs());
            queue.init();

            //
            // start workers
            //

            Set<SpeedPushWorker> pushWorkers = new HashSet<SpeedPushWorker>();
            for (int i=0;i < config.getNumberOfPushers();i++) {
                pushWorkers.add(new SpeedPushWorker(queue, config));
            }

            Set<SpeedPopWorker> popWorkers = new HashSet<SpeedPopWorker>();
            for (int i=0;i < config.getNumberOfPoppers();i++) {
                popWorkers.add(new SpeedPopWorker(queue, config));
            }

            ExecutorService pusherExecSrvc = Executors.newFixedThreadPool(config.getNumberOfPushers()+config.getNumberOfPoppers(), new ThreadFactory() {
                @Override
                public Thread newThread(Runnable runnable) {
                    Thread t = new Thread(runnable);
                    t.setName("SpeedTest-Pusher");
                    return t;
                }
            });

            ExecutorService popperExecSrvc = Executors.newFixedThreadPool(config.getNumberOfPushers()+config.getNumberOfPoppers(), new ThreadFactory() {
                @Override
                public Thread newThread(Runnable runnable) {
                    Thread t = new Thread(runnable);
                    t.setName("SpeedTest-Popper");
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

            //
            // wait for pushers to finish
            //

            pusherExecSrvc.shutdown();
            pusherExecSrvc.awaitTermination(config.getDurationOfTest()*2, TimeUnit.SECONDS);
            long endPushing = System.currentTimeMillis();

            // tell poppers, all pushers are finished
            for (SpeedPopWorker sw : popWorkers) {
                sw.stopWhenQueueEmpty();
            }

            //
            // wait for poppers to finish
            //

            while (!queue.isEmpty()) {
                System.out.println("remaing in queue = " + queue.size());
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException e) {
                    // ignore
                    Thread.interrupted();
                }
            }

            popperExecSrvc.shutdown();
            popperExecSrvc.awaitTermination(config.getDurationOfTest()*2, TimeUnit.SECONDS);
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
            System.out.println("pop duration = " + popDuration);
            System.out.println();
            System.out.println("pushed = " + numberOfPushes);
            System.out.println("popped = " + numberOfPops);
            System.out.println();
            System.out.println("push entries/sec = " + numberOfPushes/(pushDuration/1000f));
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
