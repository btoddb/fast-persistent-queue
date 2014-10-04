package com.btoddb.fastpersitentqueue.chronicle.plunkers;

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
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;


public class FilePlunkerImplTest {
    File baseDir;
    Config config = new Config();

    @Before
    public void setup() {
        baseDir = new File("tmp/" + UUID.randomUUID().toString());
    }

    @After
    public void cleanup() {
        try {
            FileUtils.deleteDirectory(baseDir);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCompileNoTokens() {
        List<FilePlunkerImpl.TokenizingPart> list = new FilePlunkerImpl().compileFilePattern("this is the string");
        assertThat(list, hasSize(1));
        assertThat(list.get(0), is(instanceOf(FilePlunkerImpl.StringPart.class)));
        assertThat(list.get(0).part, is("this is the string"));
    }

    @Test
    public void testCompileWithSingleToken() {
        List<FilePlunkerImpl.TokenizingPart> list = new FilePlunkerImpl().compileFilePattern("this ${is} the string");
        assertThat(list, hasSize(3));
        assertThat(list.get(0), is(instanceOf(FilePlunkerImpl.StringPart.class)));
        assertThat(list.get(0).part, is("this "));
        assertThat(list.get(1), is(instanceOf(FilePlunkerImpl.TokenPart.class)));
        assertThat(list.get(1).part, is("is"));
        assertThat(list.get(2), is(instanceOf(FilePlunkerImpl.StringPart.class)));
        assertThat(list.get(2).part, is(" the string"));
    }

    @Test
    public void testCompileWithOnlyAToken() {
        List<FilePlunkerImpl.TokenizingPart> list = new FilePlunkerImpl().compileFilePattern("${token}");
        assertThat(list, hasSize(1));
        assertThat(list.get(0), is(instanceOf(FilePlunkerImpl.TokenPart.class)));
        assertThat(list.get(0).part, is("token"));
    }

    @Test
    public void testCreateFileName() throws Exception {
        FilePlunkerImpl plunker = new FilePlunkerImpl();
        plunker.setFilePattern("tmp/${customer}/file");
        plunker.init(new Config());

        String path = plunker.createFileName(new FpqEvent("the-body", true)
                                       .addHeader("customer", "the-customer")
                                       .addHeader("foo", "bar"));


        assertThat(path, is("tmp/the-customer/file"));
    }

    @Test
    public void testCompileWithMultipleTokens() {
        List<FilePlunkerImpl.TokenizingPart> list = new FilePlunkerImpl().compileFilePattern("${this} string ${is} the string ${tokenizer}");
        assertThat(list, hasSize(5));
        assertThat(list.get(0), is(instanceOf(FilePlunkerImpl.TokenPart.class)));
        assertThat(list.get(0).part, is("this"));
        assertThat(list.get(1), is(instanceOf(FilePlunkerImpl.StringPart.class)));
        assertThat(list.get(1).part, is(" string "));
        assertThat(list.get(2), is(instanceOf(FilePlunkerImpl.TokenPart.class)));
        assertThat(list.get(2).part, is("is"));
        assertThat(list.get(3), is(instanceOf(FilePlunkerImpl.StringPart.class)));
        assertThat(list.get(3).part, is(" the string "));
        assertThat(list.get(4), is(instanceOf(FilePlunkerImpl.TokenPart.class)));
        assertThat(list.get(4).part, is("tokenizer"));
    }

    @Test
    public void testMultipleFileNamesCreated() throws Exception {
        FilePlunkerImpl plunker = new FilePlunkerImpl();
        plunker.setFilePattern(createFilename("${customer}/logs"));
        plunker.init(new Config());

        List<FpqEvent> eventList = new ArrayList<>();
        eventList.add(new FpqEvent("one-body", true).addHeader("customer", "one"));
        eventList.add(new FpqEvent("two-body", true).addHeader("customer", "two"));
        eventList.add(new FpqEvent("three-body", true).addHeader("customer", "three"));
        eventList.add(new FpqEvent("four-body", true).addHeader("customer", "four"));
        plunker.handleInternal(eventList);

        assertThat(createFileObj("one/logs").exists(), is(true));
        assertThat(createFileObj("two/logs").exists(), is(true));
        assertThat(createFileObj("three/logs").exists(), is(true));
        assertThat(createFileObj("four/logs").exists(), is(true));

        assertThat(createFileObj("one/logs"), hasEvents(new FpqEvent[] {eventList.get(0)}));
        assertThat(createFileObj("two/logs"), hasEvents(new FpqEvent[] {eventList.get(1)}));
        assertThat(createFileObj("three/logs"), hasEvents(new FpqEvent[] {eventList.get(2)}));
        assertThat(createFileObj("four/logs"), hasEvents(new FpqEvent[] {eventList.get(3)}));
    }

    @Test
    public void testEvictionClose() throws Exception {
        FilePlunkerImpl plunker = new FilePlunkerImpl();
        plunker.setInactiveTimeout(1);
        plunker.setFilePattern(createFilename("${customer}/logs"));
        plunker.init(new Config());

        plunker.handleInternal(Collections.singletonList(new FpqEvent("one-body", true).addHeader("customer", "one")));

        long endTime = System.currentTimeMillis()+5000;
        while (System.currentTimeMillis() < endTime && 0 < plunker.getPrintWriterCache().size()) {
            plunker.getPrintWriterCache().cleanUp();
            Thread.sleep(200);
        }
        plunker.getPrintWriterCache().cleanUp();
        assertThat(plunker.getPrintWriterCache().getIfPresent(createFilename("one/logs")), is(nullValue()));
        assertThat(plunker.getPrintWriterCache().size(), is(0L));
    }

    // ----------

    String createFilename(String fn) {
        return createFileObj(fn).getPath();
    }

    File createFileObj(String fn) {
        return new File(baseDir, fn);
    }

    Matcher<File> hasEvents(final FpqEvent[] targetEvents) {
        return new TypeSafeMatcher<File>() {
            String errorDesc;
            String expected;
            String got;

            @Override
            protected boolean matchesSafely(final File f) {
                FileReader fr = null;
                try {
                    fr  = new FileReader(f);
                    List<String> lines = IOUtils.readLines(fr);
                    if (targetEvents.length != lines.size()) {
                        errorDesc = "number of events: ";
                        expected = ""+targetEvents.length;
                        got = ""+lines.size();
                        return false;
                    }

                    for (int i=0;i < targetEvents.length;i++) {
                        FpqEvent event = config.getObjectMapper().readValue(lines.get(i), FpqEvent.class);
                        if (!targetEvents[i].equals(event)) {
                            errorDesc = "event: ";
                            expected = config.getObjectMapper().writeValueAsString(targetEvents[i]);
                            got = lines.get(i);
                            return false;
                        }
                    }
                    return true;
                }
                catch (FileNotFoundException e) {
                    e.printStackTrace();
                    return false;
                }
                catch (IOException e) {
                    e.printStackTrace();
                    return false;
                }
                finally {
                    if (null != fr) {
                        try {
                            fr.close();
                        }
                        catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }

            }

            @Override
            public void describeTo(final Description description) {
                description.appendText(errorDesc).appendValue(expected);
            }

            @Override
            protected void describeMismatchSafely(final File item, final Description mismatchDescription) {
                mismatchDescription.appendText("  was: ").appendValue(got);
            }
        };
    }

}
