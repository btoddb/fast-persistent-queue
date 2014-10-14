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

import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;


public class FpqEventTest {
    Config config = new Config();

    @Test
    public void testInstantiateWithBody() {
        FpqEvent event = new FpqEvent("the-body");
        assertThat(event.getBody(), is("the-body".getBytes()));
    }

    @Test
    public void testInstantiateWithNuyllBody() {
        FpqEvent event = new FpqEvent((byte[])null);
        assertThat(event.getBody(), is(nullValue()));
        event.getBodyAsString();
    }

    @Test
    public void testGetBodyAsStringString() throws Exception {
        FpqEvent event = new FpqEvent(Collections.singletonMap("foo", "bar"), "a-string", true);
        assertThat(event.getBodyAsString(), is("a-string"));
    }

    @Test
    public void testSerializeWithStringBody() throws Exception {
        FpqEvent event = new FpqEvent(Collections.singletonMap("foo", "bar"), "the-body", true);
        String json = config.getObjectMapper().writeValueAsString(event);
        assertThat(json, is("{\"headers\":{\"foo\":\"bar\"},\"bodyIsText\":true,\"body\":\"the-body\"}"));
    }

    @Test
    public void testDeserializeWithStringBody() throws Exception {
        FpqEvent event = config.getObjectMapper().readValue("{\"headers\":{\"foo\":\"bar\"},\"body\":\"the-body\",\"bodyIsText\":true}", FpqEvent.class);
        assertThat(event.getBodyAsString(), is("the-body"));
        assertThat(event.getHeaders(), hasEntry("foo", "bar"));
    }

    @Test
    public void testSerializeWithBytesBody() throws Exception {
        FpqEvent event = new FpqEvent(Collections.singletonMap("foo", "bar"), new byte[] {0,1,2,3});
        String json = config.getObjectMapper().writeValueAsString(event);
        assertThat(json, is("{\"headers\":{\"foo\":\"bar\"},\"bodyIsText\":false,\"body\":\"AAECAw==\"}"));
    }

    @Test
    public void testDeserializeWithBytesBody() throws Exception {
        FpqEvent event = config.getObjectMapper().readValue("{\"headers\":{\"foo\":\"bar\"},\"body\":\"AAECAw==\",\"bodyIsText\":false}", FpqEvent.class);
        assertThat(event.getBody(), is(new byte[] {0,1,2,3}));
        assertThat(event.getHeaders(), hasEntry("foo", "bar"));
    }
}