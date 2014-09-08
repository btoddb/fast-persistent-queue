package com.btoddb.fastpersitentqueue;

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

import com.eaio.uuid.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.*;

import static org.hamcrest.MatcherAssert.assertThat;


/**
 *
 */
public class MemorySegmentSerializerTest {
    File theDir;
    MemorySegmentSerializer serializer;
    AtomicLong idGen = new AtomicLong();

    @Test
    public void testSaveThenLoad() throws Exception {
        Collection<FpqEntry> entries = new LinkedList<FpqEntry>();
        entries.add(new FpqEntry(idGen.incrementAndGet(), new byte[] {0, 1, 2}));
        entries.add(new FpqEntry(idGen.incrementAndGet(), new byte[] {3, 4, 5}));
        entries.add(new FpqEntry(idGen.incrementAndGet(), new byte[] {6, 7, 8}));

        MemorySegment seg1 = new MemorySegment();
        seg1.setId(new UUID());
        seg1.setMaxSizeInBytes(1000);
        seg1.setStatus(MemorySegment.Status.READY);
        seg1.push(entries, 10);

        serializer.saveToDisk(seg1);

        Collection<File> files = FileUtils.listFiles(theDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
        assertThat(files.size(), is(1));

        MemorySegment seg2 = serializer.loadFromDisk(files.iterator().next().getName());
        assertThat(seg2.getId(), is(seg1.getId()));
        assertThat(seg2.getQueue().size(), is(seg1.getQueue().size()));
        assertThat(seg2.getMaxSizeInBytes(), is(1000L));
        assertThat(seg2.getNumberOfOnlineEntries(), is(3L));
        assertThat(seg2.getNumberOfEntries(), is(3L));
    }

    @Test
    public void testSetDirectory() throws Exception {
        assertThat(serializer.getDirectory(), is(theDir.getCanonicalFile()));
    }

    // --------------

    @Before
    public void setup() throws IOException {
        theDir = new File("junitTmp_"+new UUID().toString());
        FileUtils.forceMkdir(theDir);

        serializer = new MemorySegmentSerializer();
        serializer.setDirectory(theDir.getCanonicalFile());
    }

    @After
    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(theDir);
    }
}
