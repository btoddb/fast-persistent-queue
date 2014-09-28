package com.btoddb.fastpersitentqueue.eventbus;

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

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;


public class RestEndpointImplTest {

    @Test
    public void testIsJsonArrayWithWhitespace() {
        String json = " \t  \r  \n   [adafd]";

        RestEndpointImpl restEndpoint = new RestEndpointImpl();

        assertThat(restEndpoint.isJsonArray(new ByteArrayInputStream(json.getBytes())), is(true));
    }

    @Test
    public void testIsJsonArrayNoWhitespace() {
        String json = "[adafd]";

        RestEndpointImpl restEndpoint = new RestEndpointImpl();

        assertThat(restEndpoint.isJsonArray(new ByteArrayInputStream(json.getBytes())), is(true));
    }

    @Test
    public void testIsJsonArrayNotArrayWithWhitespace() {
        String json = "   \r  \t   \n   adafd";

        RestEndpointImpl restEndpoint = new RestEndpointImpl();

        assertThat(restEndpoint.isJsonArray(new ByteArrayInputStream(json.getBytes())), is(false));
    }

    @Test
    public void testIsJsonArrayNotArrayNoWhitespace() {
        String json = "adafd";

        RestEndpointImpl restEndpoint = new RestEndpointImpl();

        assertThat(restEndpoint.isJsonArray(new ByteArrayInputStream(json.getBytes())), is(false));
    }

    @Test
    public void testIsJsonArrayAllWhiteSpace() {
        String json = "      ";

        RestEndpointImpl restEndpoint = new RestEndpointImpl();

        assertThat(restEndpoint.isJsonArray(new ByteArrayInputStream(json.getBytes())), is(false));
    }

    @Test
    public void testIsJsonArrayEmptyString() {
        String json = "";

        RestEndpointImpl restEndpoint = new RestEndpointImpl();

        assertThat(restEndpoint.isJsonArray(new ByteArrayInputStream(json.getBytes())), is(false));
    }

    @Test
    public void testIsJsonArrayNullString() {
        RestEndpointImpl restEndpoint = new RestEndpointImpl();

        assertThat(restEndpoint.isJsonArray(null), is(false));
    }

    @Test
    public void testIsJsonArrayTooMuchWhitespace() {
        RestEndpointImpl restEndpoint = new RestEndpointImpl();

        assertThat(restEndpoint.isJsonArray(new ByteArrayInputStream(RandomStringUtils.randomAlphabetic(120).getBytes())), is(false));
    }

}