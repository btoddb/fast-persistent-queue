package com.btoddb.fastpersitentqueue.chronicle.serializers;

import com.btoddb.fastpersitentqueue.chronicle.FpqEvent;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;


/**
 * Interface for FpqEvent serializers.  Serializers are used to transform an {@link com.btoddb.fastpersitentqueue.chronicle.FpqEvent}
 * object into a byte stream.
 */
public interface FpqEventSerializer {
    void serialize(FSDataOutputStream outStream, FpqEvent event) throws IOException;
}
