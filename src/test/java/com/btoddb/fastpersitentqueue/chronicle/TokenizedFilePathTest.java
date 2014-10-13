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

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;


public class TokenizedFilePathTest {

    @Test
    public void testCompileNoTokens() {
        List<TokenizedFilePath.TokenizingPart> tokenizer = new TokenizedFilePath("this is the string").compileFilePattern();
        assertThat(tokenizer, hasSize(1));
        assertThat(tokenizer.get(0), is(instanceOf(TokenizedFilePath.StringPart.class)));
        assertThat(tokenizer.get(0).part, is("this is the string"));
    }

    @Test
    public void testCompileWithSingleToken() {
        List<TokenizedFilePath.TokenizingPart> tokenizer = new TokenizedFilePath("this ${is} the string").compileFilePattern();
        assertThat(tokenizer, hasSize(3));
        assertThat(tokenizer.get(0), is(instanceOf(TokenizedFilePath.StringPart.class)));
        assertThat(tokenizer.get(0).part, is("this "));
        assertThat(tokenizer.get(1), is(instanceOf(TokenizedFilePath.TokenPart.class)));
        assertThat(tokenizer.get(1).part, is("is"));
        assertThat(tokenizer.get(2), is(instanceOf(TokenizedFilePath.StringPart.class)));
        assertThat(tokenizer.get(2).part, is(" the string"));
    }

    @Test
    public void testCompileWithOnlyAToken() {
        List<TokenizedFilePath.TokenizingPart> tokenizer = new TokenizedFilePath("${token}").compileFilePattern();
        assertThat(tokenizer, hasSize(1));
        assertThat(tokenizer.get(0), is(instanceOf(TokenizedFilePath.TokenPart.class)));
        assertThat(tokenizer.get(0).part, is("token"));
    }

    @Test
    public void testCreateFileName() throws Exception {
        TokenizedFilePath tokenizer = new TokenizedFilePath("tmp/${customer}/file");

        String path = tokenizer.createFileName(new FpqEvent("the-body", true)
                                                       .withHeader("customer", "the-customer")
                                                       .withHeader("foo", "bar").getHeaders());


        assertThat(path, is("tmp/the-customer/file"));
    }

    @Test
    public void testCreateFileNameUnknownToken() throws Exception {
        TokenizedFilePath tokenizer = new TokenizedFilePath("tmp/${customer}/file-${timestamp}");

        String path = tokenizer.createFileName(new FpqEvent("the-body", true)
                                                       .withHeader("customer", "the-customer")
                                                       .withHeader("foo", "bar").getHeaders());


        assertThat(path, is("tmp/the-customer/file-${timestamp}"));
    }

    @Test
    public void testCompileWithMultipleTokens() {
        List<TokenizedFilePath.TokenizingPart> tokenizer = new TokenizedFilePath("${this} string ${is} the string ${tokenizer}").compileFilePattern();
        assertThat(tokenizer, hasSize(5));
        assertThat(tokenizer.get(0), is(instanceOf(TokenizedFilePath.TokenPart.class)));
        assertThat(tokenizer.get(0).part, is("this"));
        assertThat(tokenizer.get(1), is(instanceOf(TokenizedFilePath.StringPart.class)));
        assertThat(tokenizer.get(1).part, is(" string "));
        assertThat(tokenizer.get(2), is(instanceOf(TokenizedFilePath.TokenPart.class)));
        assertThat(tokenizer.get(2).part, is("is"));
        assertThat(tokenizer.get(3), is(instanceOf(TokenizedFilePath.StringPart.class)));
        assertThat(tokenizer.get(3).part, is(" the string "));
        assertThat(tokenizer.get(4), is(instanceOf(TokenizedFilePath.TokenPart.class)));
        assertThat(tokenizer.get(4).part, is("tokenizer"));
    }

}