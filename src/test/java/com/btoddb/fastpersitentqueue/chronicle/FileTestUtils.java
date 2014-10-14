package com.btoddb.fastpersitentqueue.chronicle;

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

import org.apache.commons.io.IOUtils;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;


/**
 *
 */
public class FileTestUtils {
    Config config;

    public FileTestUtils(Config config) {
        this.config = config;
    }

    public Matcher<File> hasEvent(final FpqEvent event) {
        return hasEvents(new FpqEvent[] { event });
    }

    public Matcher<File> exists() {
        return new TypeSafeMatcher<File>() {
            String errorDesc;
            String expected;
            String got;

            @Override
            protected boolean matchesSafely(final File f) {
                return f.exists();
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

    public Matcher<File> hasEvents(final FpqEvent[] targetEvents) {
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

    public Matcher<? super File> hasCount(final int count) {
        return new TypeSafeMatcher<File>() {
            String errorDesc;
            String expected;
            String got;

            @Override
            protected boolean matchesSafely(final File f) {
                FileReader fr = null;
                try {
                    fr  = new FileReader(f);
                    return count == IOUtils.readLines(fr).size();
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

    public Matcher<? super File> numWithSuffix(final String suffix, final int count) {
        return new TypeSafeMatcher<File>() {
            String errorDesc;
            String expected;
            String got;

            @Override
            protected boolean matchesSafely(final File dir) {
                String[] files = dir.list(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return name.endsWith(suffix);
                    }
                });
                return files.length == count;
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
