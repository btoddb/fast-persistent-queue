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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


/**
 *
 */
public class FpqEvent {
    private Map<String, String> headers = new HashMap<String, String>();
    private byte[] body;

    public FpqEvent() {}

    public FpqEvent(Map<String, String> headers, String body) {
        this(headers, body.getBytes());
    }

    public FpqEvent(Map<String, String> headers, byte[] body) {
        this.headers = headers;
        this.body = body;
    }

    public FpqEvent addHeader(String name, String value) {
        headers.put(name, value);
        return this;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    public byte[] getBody() {
        return body;
    }

    public String getBodyAsString() {
        return new String(body);
    }

    public void setBodyAsString(String body) {
        this.body = body.getBytes();
    }

    @Override
    public String toString() {
        return "FpqBusEvent{" +
                "headers=" + headers +
                ", body='" + getBodyAsString() + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FpqEvent that = (FpqEvent) o;

        if (!Arrays.equals(body, that.body)) {
            return false;
        }
        if (headers != null ? !headers.equals(that.headers) : that.headers != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = headers != null ? headers.hashCode() : 0;
        result = 31 * result + (body != null ? Arrays.hashCode(body) : 0);
        return result;
    }
}
