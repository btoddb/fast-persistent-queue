package com.btoddb.fastpersitentqueue;

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

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;


public class FpqBatchReaderIT {
    Fpq fpq1;
    File theDir;

    @Before
    public void setup() throws IOException {
        theDir = new File("junitTmp_"+ UUID.randomUUID().toString());
        FileUtils.forceMkdir(theDir);

        fpq1 = new Fpq();
        fpq1.setQueueName("fpq1");
        fpq1.setMaxTransactionSize(1000);
        fpq1.setMaxMemorySegmentSizeInBytes(100000);
        fpq1.setMaxJournalFileSize(10000);
        fpq1.setMaxJournalDurationInMs(30000);
        fpq1.setFlushPeriodInMs(1000);
        fpq1.setNumberOfFlushWorkers(4);
        fpq1.setJournalDirectory(new File(new File(theDir, "fp1"), "journal"));
        fpq1.setPagingDirectory(new File(new File(theDir, "fp1"), "paging"));
    }

    @After
    public void cleanup() throws IOException {
        if (null != fpq1) {
            try {
                fpq1.shutdown();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    FileUtils.deleteDirectory(theDir);
    }

    @Test
    public void testRead() throws Exception {
        int numEvents = 500; // multiples of 100 please

        fpq1.init();

        final List<FpqEntry> events = new ArrayList<FpqEntry>(numEvents);
        FpqBatchReader br = new FpqBatchReader();
        br.setFpq(fpq1);
        br.setMaxBatchSize(numEvents / 10);
        br.setMaxBatchWaitInMs(10);
        br.setSleepTimeBetweenPopsInMs(25);
        br.setCallback(new FpqBatchCallback() {
            @Override
            public void available(Collection<FpqEntry> entries) throws Exception {
                events.addAll(entries);
            }
        });

        br.init();

        for (int j=0;j < numEvents/10;j++) {
            fpq1.beginTransaction();
            for (int i = 0; i < 10; i++) {
                fpq1.push(String.format("msg %04d", 10*j+i).getBytes());
            }
            fpq1.commit();
            Thread.sleep(50);
        }

        long endTime = System.currentTimeMillis()+5000;
        while (events.size() < numEvents && System.currentTimeMillis() < endTime) {
            Thread.sleep(100);
        }

        br.shutdown();

        assertThat(events, hasSize(numEvents));

        for (int i = 0; i < numEvents; i++) {
            assertThat(new String(events.get(i).getData()), is(String.format("msg %04d", i)));
        }
    }

    @Ignore
    @Test
    public void testBatchReaderNotInitialized() {

    }

    @Ignore
    @Test
    public void testFpqNotInitialized() {

    }

    @Ignore
    @Test
    public void testOtherFailConditions() {

    }
}
