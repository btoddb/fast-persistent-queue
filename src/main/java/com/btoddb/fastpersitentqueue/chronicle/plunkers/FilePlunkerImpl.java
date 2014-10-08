package com.btoddb.fastpersitentqueue.chronicle.plunkers;

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

import com.btoddb.fastpersitentqueue.Utils;
import com.btoddb.fastpersitentqueue.chronicle.Config;
import com.btoddb.fastpersitentqueue.chronicle.FpqEvent;
import com.btoddb.fastpersitentqueue.chronicle.TokenizedFilePath;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


/**
 *
 */
public class FilePlunkerImpl extends PlunkerBaseImpl {
    private static Logger logger = LoggerFactory.getLogger(FilePlunkerImpl.class);

    private String filePattern;
    private int inactiveTimeout = 120;
    private int maxOpenFiles = 100;

    private TokenizedFilePath tokenizedFilePath;

    Cache<String, PrintWriter> printWriterCache;

    @Override
    public void init(Config config) throws Exception {
        super.init(config);

        tokenizedFilePath = new TokenizedFilePath(filePattern);

        printWriterCache = CacheBuilder.newBuilder()
                .expireAfterAccess(inactiveTimeout, TimeUnit.SECONDS)
                .maximumSize(maxOpenFiles)
                .removalListener(new RemovalListener<String, PrintWriter>() {
                    @Override
                    public void onRemoval(RemovalNotification<String, PrintWriter> pw) {
                        pw.getValue().close();
                    }
                })
                .build();
    }

    @Override
    protected void handleInternal(Collection<FpqEvent> events) throws Exception {
        for (FpqEvent event : events) {
            PrintWriter fw = retrievePrintWriter(tokenizedFilePath.createFileName(event.getHeaders()));
            config.getObjectMapper().writeValue(fw, event);
            fw.println();
            fw.flush();
        }
    }

    PrintWriter retrievePrintWriter(final String fn) {
        try {
            PrintWriter pw = printWriterCache.get(fn, new Callable<PrintWriter>() {
                 @Override
                 public PrintWriter call() throws IOException {
                     File f = new File(fn);
                     FileUtils.forceMkdir(f.getParentFile());
                     return new PrintWriter(new FileWriter(f));
                 }
             });
            return pw;
        }
        catch (ExecutionException e) {
            Utils.logAndThrow(logger, "exception while trying to retrieve PrintWriter from cache", e);
            return null;
        }
    }


    @Override
    public void shutdown() {
        if (null != printWriterCache) {
            printWriterCache.invalidateAll();
            printWriterCache.cleanUp();
        }
    }

    public String getFilePattern() {
        return filePattern;
    }

    public void setFilePattern(String filePattern) {
        this.filePattern = filePattern;
    }

    public int getInactiveTimeout() {
        return inactiveTimeout;
    }

    public void setInactiveTimeout(int inactiveTimeout) {
        this.inactiveTimeout = inactiveTimeout;
    }

    public int getMaxOpenFiles() {
        return maxOpenFiles;
    }

    public void setMaxOpenFiles(int maxOpenFiles) {
        this.maxOpenFiles = maxOpenFiles;
    }

    Cache<String, PrintWriter> getPrintWriterCache() {
        return printWriterCache;
    }

}
