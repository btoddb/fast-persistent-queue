package com.btoddb.fastpersitentqueue.speedtest;

/*
 * #%L
 * fast-persistent-queue
 * %%
 * Copyright (C) 2014 btoddb.com
 * %%
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * #L%
 */

import com.btoddb.fastpersitentqueue.Fpq;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


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

        SpeedTestConfig config = SpeedTestConfig.create(args[0]);

        System.out.println(config.toString());

        File theDir = new File(config.getDirectory(), "speed-"+ UUID.randomUUID().toString());
        FileUtils.forceMkdir(theDir);

        Fpq queue = config.getFpq();
        queue.setJournalDirectory(new File(theDir, "journals"));
        queue.setPagingDirectory(new File(theDir, "pages"));

        try {
            queue.init();

            //
            // start workers
            //

            AtomicLong counter = new AtomicLong();
            AtomicLong pushSum = new AtomicLong();
            AtomicLong popSum = new AtomicLong();

            long startTime = System.currentTimeMillis();

            Set<SpeedPushWorker> pushWorkers = new HashSet<SpeedPushWorker>();
            for (int i=0;i < config.getNumberOfPushers();i++) {
                pushWorkers.add(new SpeedPushWorker(queue, config, counter, pushSum));
            }

            Set<SpeedPopWorker> popWorkers = new HashSet<SpeedPopWorker>();
            for (int i=0;i < config.getNumberOfPoppers();i++) {
                popWorkers.add(new SpeedPopWorker(queue, config, popSum));
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
            // wait to finish
            //

            long endTime = startTime + config.getDurationOfTest()*1000;
            long endPushing = 0;
            long displayTimer = 0;
            while (0 == endPushing || !queue.isEmpty()) {
                // display status every second
                if (1000 < (System.currentTimeMillis()-displayTimer)) {
                    System.out.println(String.format("status (%ds) : journals = %d : memory segments = %d",
                                                     (endTime - System.currentTimeMillis()) / 1000,
                                                     queue.getJournalMgr().getJournalIdMap().size(),
                                                     queue.getMemoryMgr().getSegments().size()
                    ));
                    displayTimer = System.currentTimeMillis();
                }

                pusherExecSrvc.shutdown();
                if (pusherExecSrvc.awaitTermination(100, TimeUnit.MILLISECONDS)) {
                    endPushing = System.currentTimeMillis();
                    // tell poppers, all pushers are finished
                    for (SpeedPopWorker sw : popWorkers) {
                        sw.stopWhenQueueEmpty();
                    }
                }
            }

            long endPopping = System.currentTimeMillis();

            popperExecSrvc.shutdown();
            popperExecSrvc.awaitTermination(10, TimeUnit.SECONDS);

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

            System.out.println("push - pop checksum = " + pushSum.get() + " - " + popSum.get() + " = " + (pushSum.get()-popSum.get()));
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
