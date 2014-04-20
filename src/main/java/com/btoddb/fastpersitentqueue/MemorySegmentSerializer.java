package com.btoddb.fastpersitentqueue;

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
        File theFile = new File(directory, segment.getId().toString());
        RandomAccessFile raFile = new RandomAccessFile(theFile, "rw");
        try {
            segment.writeToDisk(raFile);
        }
        finally {
            raFile.close();
        }
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
