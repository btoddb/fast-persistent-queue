package com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs;

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
