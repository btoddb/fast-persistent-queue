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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;


/**
 *
 */
public class MemorySegmentSerializer {
    private static Logger logger = LoggerFactory.getLogger(MemorySegmentSerializer.class);

    // should be it's on directory on separate spindle from journal
    public File directory;


    // any synchronizing should have been done above call
    public void saveToDisk(MemorySegment segment) throws IOException {
        File theFile = createPagingFile(segment);
        RandomAccessFile raFile = new RandomAccessFile(theFile, "rw");
        try {
            segment.writeToDisk(raFile);
        }
        finally {
            raFile.close();
        }
    }

    private File createPagingFile(MemorySegment segment) {
        return new File(directory, segment.getId().toString());
    }

    public MemorySegment loadFromDisk(String fn) throws IOException {
        MemorySegment segment = new MemorySegment();
        File theFile = new File(directory, fn);
        loadFromDisk(theFile, segment);
        return segment;
    }

    public void loadFromDisk(MemorySegment segment) throws IOException {
        File theFile = new File(directory, segment.getId().toString());
        loadFromDisk(theFile, segment);
    }

    public void loadFromDisk(File theFile, MemorySegment segment) throws IOException {
        RandomAccessFile raFile = new RandomAccessFile(theFile, "r");
        try {
            segment.readFromPagingFile(raFile);
        }
        finally {
            raFile.close();
        }
    }

    public MemorySegment loadHeaderOnly(String fn) throws IOException {
        File theFile = new File(directory, fn);
        RandomAccessFile raFile = new RandomAccessFile(theFile, "r");
        try {
            MemorySegment segment = new MemorySegment();
            segment.readHeaderFromDisk(raFile);
            return segment;
        }
        finally {
            raFile.close();
        }
    }

    public void removePagingFile(MemorySegment segment) {
        File theFile = new File(directory, segment.getId().toString());
        try {
            FileUtils.forceDelete(theFile);
            logger.debug("removed memory paging file {}", theFile.toString());
        }
        catch (FileNotFoundException e) {
            try {
                logger.debug("File not found (this is normal) while removing memory paging file, {}", theFile.getCanonicalFile());
            }
            catch (IOException e1) {
                // ignore
            }
        }
        catch (IOException e) {
            try {
                logger.error("exception while removing memory paging file, {}", theFile.getCanonicalFile(), e);
            }
            catch (IOException e1) {
                // ignore
                logger.error("another exception while removing memory paging file", e);
            }
        }
    }

    public boolean searchOffline(MemorySegment seg, FpqEntry target) throws IOException {
        RandomAccessFile raFile = new RandomAccessFile(createPagingFile(seg), "r");
        try {
            // jump over header info - we already have it
            raFile.seek(seg.getEntryListOffsetOnDisk());
            for (int i=0;i < seg.getNumberOfEntries();i++) {
                FpqEntry entry = new FpqEntry();
                entry.readFromPaging(raFile);
                if (target.equals(entry)) {
                    return true;
                }
            }
            return false;
        }
        finally {
            raFile.close();
        }
    }

    public File getDirectory() {
        return directory;
    }

    public void setDirectory(File directory) throws IOException {
        this.directory = directory;
    }

    public void shutdown() {
        // ignore for now
    }

    public void init() throws IOException {
        FileUtils.forceMkdir(directory);
    }
}
