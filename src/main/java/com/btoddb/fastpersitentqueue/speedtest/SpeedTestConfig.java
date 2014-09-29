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
import com.btoddb.fastpersitentqueue.eventbus.FpqCatcher;
import com.btoddb.fastpersitentqueue.eventbus.FpqPlunker;
import com.btoddb.fastpersitentqueue.eventbus.routers.FpqRouter;
import com.fasterxml.jackson.databind.ObjectMapper;
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
public class SpeedTestConfig {
    private static Logger logger = LoggerFactory.getLogger(SpeedTestConfig.class);

    String configFilename;

    int durationOfTest = 20; // seconds
    int numberOfPushers = 4;
    int numberOfPoppers = 4;
    int entrySize = 1000;
    int maxTransactionSize = 2000;
    int pushBatchSize = 1;
    int popBatchSize = 2000;
    String directory;
    Fpq fpq;


    public static SpeedTestConfig create(String configFilename) throws FileNotFoundException {
        Yaml yaml = new Yaml(new Constructor(SpeedTestConfig.class));
        SpeedTestConfig config;
        FileInputStream inStream = new FileInputStream(configFilename);
        try {
            config = (SpeedTestConfig) yaml.load(inStream);
            config.configFilename = configFilename;
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

    public String getConfigFilename() {
        return configFilename;
    }

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

    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    public String getDirectory() {
        return directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public Fpq getFpq() {
        return fpq;
    }

    public void setFpq(Fpq fpq) {
        this.fpq = fpq;
    }
}
