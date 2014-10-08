package com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs;

/**
 * Created by burrb009 on 10/8/14.
 */
public interface HdfsWriterFactory {

    HdfsWriter createWriter(String permFilename, String openFilename);

}
