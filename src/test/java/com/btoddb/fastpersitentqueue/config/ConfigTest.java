package com.btoddb.fastpersitentqueue.config;

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
import org.junit.Test;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;


public class ConfigTest {

    @Test
    public void testCreate() throws Exception {
        Config config = Config.create("src/test/resources/chronicle-test.yaml");
        assertThat(config, is(notNullValue()));
        assertThat(config.getCatchers().keySet(), hasSize(2));
        assertThat(config.getCatchers(), hasKey("rest-catcher"));
        assertThat(config.getCatchers(), hasKey("error-catcher"));
        assertThat(config.getPlunkers().keySet(), hasSize(3));
        assertThat(config.getPlunkers(), hasKey("test-plunker"));
        assertThat(config.getPlunkers(), hasKey("null-plunker"));
        assertThat(config.getPlunkers(), hasKey("error-plunker"));
    }
}