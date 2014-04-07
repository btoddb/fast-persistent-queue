package com.btoddb.fastpersitentqueue;

import com.eaio.uuid.UUID;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;


/**
 *
 */
public class MemorySegmentSerializer {
    // should be it's on directory on separate spindle from journal
    public File directory;


    // any synchronizing should have been done above call
    public void saveToDisk(MemorySegment segment) throws IOException {
        FileUtils.forceMkdir(directory);
        File theFile = new File(directory, segment.getId().toString());
        RandomAccessFile raFile = new RandomAccessFile(theFile, "w");
        try {
            segment.writeToDisk(raFile);
        }
        finally {
            raFile.close();
        }
    }

    public MemorySegment loadFromDisk(UUID segmentId) throws IOException {
        File theFile = new File(directory, segmentId.toString());
        MemorySegment segment = new MemorySegment();
        RandomAccessFile raFile = new RandomAccessFile(theFile, "r");
        try {
            segment.readFromDisk(raFile);
        }
        finally {
            raFile.close();
        }

        return segment;
    }

    public File getDirectory() {
        return directory;
    }

    public void setDirectory(File directory) {
        this.directory = directory;
    }
}
