package com.btoddb.fastpersitentqueue.chronicle.serializers;

import com.btoddb.fastpersitentqueue.chronicle.Config;
import com.btoddb.fastpersitentqueue.chronicle.FpqEvent;
import com.btoddb.fastpersitentqueue.chronicle.serializers.FpqEventSerializer;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;


/**
 *
 */
public class JsonSerializerImpl implements FpqEventSerializer {
    Config config;
    boolean appendNewline = true;

    public JsonSerializerImpl(Config config) {
        this.config = config;
    }

    @Override
    public void serialize(FSDataOutputStream outStream, FpqEvent event) throws IOException {
        outStream.write(config.getObjectMapper().writeValueAsBytes(event));
        if (appendNewline) {
            outStream.write('\n');
        }
    }

    public boolean isAppendNewline() {
        return appendNewline;
    }

    public void setAppendNewline(boolean appendNewline) {
        this.appendNewline = appendNewline;
    }

    public Config getConfig() {
        return config;
    }

    public void setConfig(Config config) {
        this.config = config;
    }
}
