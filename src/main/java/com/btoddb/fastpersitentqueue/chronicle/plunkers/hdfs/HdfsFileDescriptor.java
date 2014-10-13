package com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;


/**
 *
 */
public class HdfsFileDescriptor {
    private String openFilename;
    private FileSystem fileSystem;
    private FSDataOutputStream outputStream;
    private String permFilename;

    public void setOpenFilename(String filename) {
        this.openFilename = new File(filename).getAbsolutePath();
    }

    public String getOpenFilename() {
        return openFilename;
    }

    public void setPermFilename(String filename) {
        this.permFilename = filename;
    }

    public String getPermFilename() {
        return permFilename;
    }

    public void setFileSystem(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public void setOutputStream(FSDataOutputStream outputStream) {
        this.outputStream = outputStream;
    }

    public FSDataOutputStream getOutputStream() {
        return outputStream;
    }
}
