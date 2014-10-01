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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.codec.binary.Base64;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


/**
 *
 */
@JsonPropertyOrder({"headers", "bodyIsText", "body"})
public class FpqEvent {
    private Map<String, String> headers = new HashMap<String, String>();
    private final byte[] body;
    private final boolean bodyIsText;


    public FpqEvent(byte[] body) {
        this.body = body;
        bodyIsText = false;
    }

    public FpqEvent(String body, boolean bodyIsText) {
        this.bodyIsText = bodyIsText;
        if (bodyIsText) {
            this.body = body.getBytes();
        }
        else {
            this.body = Base64.decodeBase64(body);
        }
    }

    @JsonCreator
    public FpqEvent(
            @JsonProperty("headers") Map<String, String> headers,
            @JsonProperty("body") String body,
            @JsonProperty("bodyIsText") boolean bodyIsText
            ) {
        this.headers = headers;
        this.bodyIsText = bodyIsText;
        if (bodyIsText) {
            this.body = body.getBytes();
        }
        else {
            this.body = Base64.decodeBase64(body);
        }
    }

    public FpqEvent(Map<String, String> headers, byte[] body) {
        this.headers = headers;
        this.body = body;
        bodyIsText = false;
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

    @JsonGetter("body")
    public String getBodyAsString() {
        if (bodyIsText) {
            return new String(body);
        }
        else {
            return Base64.encodeBase64String(body);
        }
    }

    public boolean isBodyIsText() {
        return bodyIsText;
    }

    @Override
    public String toString() {
        return "FpqBusEvent{" +
                "headers=" + headers +
                ", body='" + getBody() + '\'' +
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
