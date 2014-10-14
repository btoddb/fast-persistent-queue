package com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs;

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

import com.btoddb.fastpersitentqueue.chronicle.Config;
import com.btoddb.fastpersitentqueue.chronicle.FpqEvent;
import com.btoddb.fastpersitentqueue.chronicle.TokenizedFilePath;
import com.btoddb.fastpersitentqueue.chronicle.serializers.FpqEventSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 *
 */
public class HdfsWriter {
    private static final Logger logger = LoggerFactory.getLogger(HdfsWriter.class);

    private Config config;
    private FpqEventSerializer serializer;

    private TokenizedFilePath permTokenizedFilePath;
    private TokenizedFilePath openTokenizedFilePath;
    private String permFilenamePattern;
    private String openFilenamePattern;

    private HdfsFileDescriptor fileDescriptor;
    private ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();


    public void init(Config config) throws IOException {
        this.config = config;

        this.permTokenizedFilePath = new TokenizedFilePath(permFilenamePattern);
        this.openTokenizedFilePath = new TokenizedFilePath(openFilenamePattern);

        HdfsFileDescriptor desc = new HdfsFileDescriptor();

        Map<String, String> fileNameParams = Collections.singletonMap("timestamp", String.valueOf(System.currentTimeMillis()));
        desc.setOpenFilename(openTokenizedFilePath.createFileName(fileNameParams));
        desc.setPermFilename(permTokenizedFilePath.createFileName(fileNameParams));

        Configuration conf = new Configuration();
        Path path = new Path(desc.getOpenFilename());
        desc.setFileSystem(path.getFileSystem(conf));

        desc.setOutputStream(desc.getFileSystem().create(path));
        fileDescriptor = desc;
    }

    public void write(FpqEvent event) throws IOException {
        closeLock.readLock().lock();
        try {
            serializer.serialize(fileDescriptor.getOutputStream(), event);
        }
        finally {
            closeLock.readLock().unlock();
        }
    }

    public void flush() throws IOException {
        fileDescriptor.getOutputStream().hflush();
        // TODO:BTB - not sure if i need to flush+hsync
        fileDescriptor.getOutputStream().hsync();
    }

    public void close() throws IOException {
        closeLock.writeLock().lock();
        try {
            if (null != fileDescriptor.getOutputStream()) {
                fileDescriptor.getOutputStream().close();
                fileDescriptor.setOutputStream(null);
            }
        }
        finally {
            closeLock.writeLock().unlock();
        }

        renameToPerm();
    }

    void renameToPerm() throws IOException {
        fileDescriptor.getFileSystem().rename(new Path(fileDescriptor.getOpenFilename()), new Path(fileDescriptor.getPermFilename()));
    }

    public FpqEventSerializer getSerializer() {
        return serializer;
    }

    public void setSerializer(FpqEventSerializer serializer) {
        this.serializer = serializer;
    }

    String getCurrentFilename() {
        return fileDescriptor.getOpenFilename();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HdfsWriter that = (HdfsWriter) o;

        if (openTokenizedFilePath != null ? !openTokenizedFilePath.equals(that.openTokenizedFilePath) : that.openTokenizedFilePath != null) {
            return false;
        }
        if (permTokenizedFilePath != null ? !permTokenizedFilePath.equals(that.permTokenizedFilePath) : that.permTokenizedFilePath != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = permTokenizedFilePath != null ? permTokenizedFilePath.hashCode() : 0;
        result = 31 * result + (openTokenizedFilePath != null ? openTokenizedFilePath.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "HdfsWriter{" +
                "permTokenizedFilePath=" + permTokenizedFilePath +
                ", openTokenizedFilePath=" + openTokenizedFilePath +
                '}';
    }

    public void setPermFilenamePattern(String permFilenamePattern) {
        this.permFilenamePattern = permFilenamePattern;
    }

    public String getPermFilenamePattern() {
        return permFilenamePattern;
    }

    public void setOpenFilenamePattern(String openFilenamePattern) {
        this.openFilenamePattern = openFilenamePattern;
    }

    public String getOpenFilenamePattern() {
        return openFilenamePattern;
    }

    public HdfsFileDescriptor getFileDescriptor() {
        return fileDescriptor;
    }

    public void setFileDescriptor(HdfsFileDescriptor fileDescriptor) {
        this.fileDescriptor = fileDescriptor;
    }
}
