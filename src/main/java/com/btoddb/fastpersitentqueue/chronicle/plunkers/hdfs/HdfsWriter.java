package com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs;

import com.btoddb.fastpersitentqueue.chronicle.Config;
import com.btoddb.fastpersitentqueue.chronicle.FpqEvent;
import com.btoddb.fastpersitentqueue.chronicle.TokenizedFilePath;
import com.btoddb.fastpersitentqueue.chronicle.serializers.FpqEventSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;


/**
 *
 */
public class HdfsWriter {
    private static final Logger logger = LoggerFactory.getLogger(HdfsWriter.class);

    private Config config;

    private FpqEventSerializer serializer;
    private String currentFilename;
    private FileSystem fileSystem;
    private Path path;
    private FSDataOutputStream outStream;

    private TokenizedFilePath permTokenizedFilePath;
    private TokenizedFilePath openTokenizedFilePath;
    private String permFilenamePattern;
    private String openFilenamePattern;


    public void init(Config config) {
        this.config = config;

        this.permTokenizedFilePath = new TokenizedFilePath(permFilenamePattern);
        this.openTokenizedFilePath = new TokenizedFilePath(openFilenamePattern);
    }

    public void open() throws IOException {
        long now = System.currentTimeMillis();
        currentFilename = openTokenizedFilePath.createFileName(Collections.singletonMap("timestamp", String.valueOf(now)));

        Configuration conf = new Configuration();
        path = new Path(currentFilename);
        fileSystem = path.getFileSystem(conf);

        outStream = fileSystem.create(path);

//        Path dstPath, FileSystem hdfs) throws
//            if(useRawLocalFileSystem) {
//                if(hdfs instanceof LocalFileSystem) {
//                    hdfs = ((LocalFileSystem)hdfs).getRaw();
//                } else {
//                    logger.warn("useRawLocalFileSystem is set to true but file system " +
//                                        "is not of type LocalFileSystem: " + hdfs.getClass().getName());
//                }
//            }
//
//            boolean appending = false;
//            if (conf.getBoolean("hdfs.append.support", false) == true && hdfs.isFile
//                    (dstPath)) {
//                outStream = hdfs.append(dstPath);
//                appending = true;
//            } else {
//                outStream = hdfs.create(dstPath);
//            }
//
//            serializer = EventSerializerFactory.getInstance(
//                    serializerType, serializerContext, outStream);
//            if (appending && !serializer.supportsReopen()) {
//                outStream.close();
//                serializer = null;
//                throw new IOException("serializer (" + serializerType +
//                                              ") does not support append");
//            }
//
//            // must call superclass to check for replication issues
//            registerCurrentStream(outStream, hdfs, dstPath);
//
//            if (appending) {
//                serializer.afterReopen();
//            } else {
//                serializer.afterCreate();
//            }

    }

    public void write(FpqEvent event) throws IOException {
        serializer.serialize(outStream, event);
    }

    public void flush() throws IOException {
        outStream.flush();
        outStream.sync();
    }

    public void close() throws IOException {
        flush();
        if (null != outStream) {
            outStream.close();
        }
    }

    public FpqEventSerializer getSerializer() {
        return serializer;
    }

    public void setSerializer(FpqEventSerializer serializer) {
        this.serializer = serializer;
    }

    public String getCurrentFilename() {
        return currentFilename;
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
}
