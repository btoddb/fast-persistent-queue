package com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs;

import com.btoddb.fastpersitentqueue.chronicle.Config;
import com.btoddb.fastpersitentqueue.chronicle.FpqEvent;
import com.btoddb.fastpersitentqueue.chronicle.serializers.FpqEventSerializer;


/**
 *
 */
public class HdfsWriterFacoryImpl implements HdfsWriterFactory {
    private Config config;
    private FpqEventSerializer serializer;

    public HdfsWriterFacoryImpl(Config config, FpqEventSerializer serializer) {
        this.config = config;
        this.serializer = serializer;
    }

    @Override
    public HdfsWriter createWriter(String permFilename, String openFilename) {
        // get the tokenized file paths, which themselves will still have tokens (ie  timestamp)
        final HdfsWriter writer = new HdfsWriter();
        writer.setSerializer(serializer);
        writer.setPermFilenamePattern(permFilename);
        writer.setOpenFilenamePattern(openFilename);
        return writer;
    }
}
