package com.btoddb.fastpersitentqueue.config;

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

import com.btoddb.fastpersitentqueue.eventbus.FpqCatcher;
import com.btoddb.fastpersitentqueue.eventbus.FpqPlunker;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;


/**
 *
 */
public class Config {
    private static Logger logger = LoggerFactory.getLogger(Config.class);

//    String configFilename;

    int durationOfTest = 20; // seconds
    int numberOfPushers = 4;
    int numberOfPoppers = 4;
    int entrySize = 1000;
    int maxTransactionSize = 2000;
    int pushBatchSize = 1;
    int popBatchSize = 2000;
    long maxMemorySegmentSizeInBytes = 10000000;
    int maxJournalFileSize = 10000000;
    int journalMaxDurationInMs = 30000;
    int flushPeriodInMs = 1000;
    int numberOfFlushWorkers = 4;
    String directory;

    Collection<FpqCatcher> catchers = new HashSet<FpqCatcher>();
    Collection<FpqPlunker> plunkers = new HashSet<FpqPlunker>();


    public static Config create(String configFilename) throws FileNotFoundException {
        Yaml yaml = new Yaml(new Constructor(Config.class));
        Config config;
        FileInputStream inStream = new FileInputStream(configFilename);
        try {
            config = (Config) yaml.load(inStream);
        }
        finally {
            try {
                inStream.close();
            }
            catch (IOException e) {
                logger.error("exception while closing config file", e);
            }
        }

        return config;
    }

//    public Config(String configFilename) throws IOException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
//        this.configFilename = configFilename;
//
//        System.out.println("reading config from : " + configFilename);
//        FileReader reader = new FileReader(configFilename);
//        try {
//            Properties props = new Properties();
//            props.load(reader);
//
//            for ( Map.Entry<Object, Object> entry : props.entrySet()) {
//                String name = (String) entry.getKey();
//                String value = (String) entry.getValue();
//
//                PropertyUtilsBean pub = new PropertyUtilsBean();
//                PropertyDescriptor propDesc;
//                try {
//                    propDesc = pub.getPropertyDescriptor(this, name);
//                }
//                catch (Exception e) {
//                    others.put(name, value);
//                    continue;
//                }
//
//                if (propDesc.getPropertyType() == int.class) {
//                    propDesc.getWriteMethod().invoke(this, Integer.valueOf(value));
//                }
//                else if(propDesc.getPropertyType() == long.class) {
//                    propDesc.getWriteMethod().invoke(this, Long.valueOf(value));
//                }
//                else {
//                    pub.setProperty(this, name, value);
//                }
//            }
//        }
//        finally {
//            reader.close();
//        }
//    }

    public int getDurationOfTest() {
        return durationOfTest;
    }

    public void setDurationOfTest(int durationOfTest) {
        this.durationOfTest = durationOfTest;
    }

    public int getNumberOfPushers() {
        return numberOfPushers;
    }

    public void setNumberOfPushers(int numberOfPushers) {
        this.numberOfPushers = numberOfPushers;
    }

    public int getNumberOfPoppers() {
        return numberOfPoppers;
    }

    public void setNumberOfPoppers(int numberOfPoppers) {
        this.numberOfPoppers = numberOfPoppers;
    }

    public int getEntrySize() {
        return entrySize;
    }

    public void setEntrySize(int entrySize) {
        this.entrySize = entrySize;
    }

    public int getMaxTransactionSize() {
        return maxTransactionSize;
    }

    public void setMaxTransactionSize(int maxTransactionSize) {
        this.maxTransactionSize = maxTransactionSize;
    }

    public int getPushBatchSize() {
        return pushBatchSize;
    }

    public void setPushBatchSize(int pushBatchSize) {
        this.pushBatchSize = pushBatchSize;
    }

    public int getPopBatchSize() {
        return popBatchSize;
    }

    public void setPopBatchSize(int popBatchSize) {
        this.popBatchSize = popBatchSize;
    }

    public long getMaxMemorySegmentSizeInBytes() {
        return maxMemorySegmentSizeInBytes;
    }

    public void setMaxMemorySegmentSizeInBytes(long maxMemorySegmentSizeInBytes) {
        this.maxMemorySegmentSizeInBytes = maxMemorySegmentSizeInBytes;
    }

    public int getMaxJournalFileSize() {
        return maxJournalFileSize;
    }

    public void setMaxJournalFileSize(int maxJournalFileSize) {
        this.maxJournalFileSize = maxJournalFileSize;
    }

    public int getJournalMaxDurationInMs() {
        return journalMaxDurationInMs;
    }

    public void setJournalMaxDurationInMs(int journalMaxDurationInMs) {
        this.journalMaxDurationInMs = journalMaxDurationInMs;
    }

    public int getFlushPeriodInMs() {
        return flushPeriodInMs;
    }

    public void setFlushPeriodInMs(int flushPeriodInMs) {
        this.flushPeriodInMs = flushPeriodInMs;
    }

    public int getNumberOfFlushWorkers() {
        return numberOfFlushWorkers;
    }

    public void setNumberOfFlushWorkers(int numberOfFlushWorkers) {
        this.numberOfFlushWorkers = numberOfFlushWorkers;
    }

    public String getDirectory() {
        return directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public Collection<FpqCatcher> getCatchers() {
        return catchers;
    }

    public void setCatchers(Collection<FpqCatcher> catchers) {
        this.catchers = catchers;
    }

    public Collection<FpqPlunker> getPlunkers() {
        return plunkers;
    }

    public void setPlunkers(Collection<FpqPlunker> plunkers) {
        this.plunkers = plunkers;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
