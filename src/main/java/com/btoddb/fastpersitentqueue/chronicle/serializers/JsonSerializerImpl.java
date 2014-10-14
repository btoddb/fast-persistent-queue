package com.btoddb.fastpersitentqueue.chronicle.serializers;

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
