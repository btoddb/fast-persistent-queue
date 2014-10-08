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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 *
 */
public class TokenizedFilePath {
    private String filePattern;
    private List<TokenizingPart> tokenizedFilename;

    public TokenizedFilePath(String filePattern) {
        this.filePattern = filePattern;
        this.tokenizedFilename = compileFilePattern();
    }

    public String createFileName(Map<String, String> valueMap) {
        StringBuilder sb = new StringBuilder();
        for (TokenizingPart part : tokenizedFilename) {
            if (part instanceof StringPart) {
                sb.append(part.part);
            }
            // if key is found in headers, then use it.  otherwise let it be
            else if (valueMap.containsKey(part.part)) {
                sb.append(valueMap.get(part.part));
            }
            else {
                sb.append("${");
                sb.append(part.part);
                sb.append("}");
            }
        }
        return sb.toString();
    }

    List<TokenizingPart> compileFilePattern() {
        List<TokenizingPart> compiledList = new ArrayList<>();

        int startIndex = 0;
        int endIndex = 0;

        while (-1 != startIndex && -1 != endIndex && endIndex < filePattern.length()) {
            // find 'start of token'
            startIndex = filePattern.indexOf("${", endIndex);

            // save the 'not-token' part
            if (-1 != startIndex) {
                if (0 < startIndex) {
                    compiledList.add(new StringPart(filePattern.substring(endIndex, startIndex)));
                }
                startIndex += 2;

                // find 'end of token'
                endIndex = filePattern.indexOf("}", startIndex);

                // replace the token
                compiledList.add(new TokenPart(filePattern.substring(startIndex, endIndex)));

                endIndex++;
            }
            else {
                compiledList.add(new StringPart(filePattern.substring(endIndex)));
            }
        }
        return compiledList;
    }

    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this, false);
    }

    public boolean equals(Object o) {
        return EqualsBuilder.reflectionEquals(this, o, false);
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    // ----------

    abstract class TokenizingPart {
        final String part;
        TokenizingPart(String part) {
            this.part = part;
        }

        public int hashCode() {
            return HashCodeBuilder.reflectionHashCode(this, false);
        }

        public boolean equals(Object o) {
            return EqualsBuilder.reflectionEquals(this, o, false);
        }

        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }

    }
    class StringPart extends TokenizingPart {
        StringPart(String part) {
            super(part);
        }

        public int hashCode() {
            return HashCodeBuilder.reflectionHashCode(this, false);
        }

        public boolean equals(Object o) {
            return EqualsBuilder.reflectionEquals(this, o, false);
        }

        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }
    }
    class TokenPart extends TokenizingPart {
        TokenPart(String part) {
            super(part);
        }

        public int hashCode() {
            return HashCodeBuilder.reflectionHashCode(this, false);
        }

        public boolean equals(Object o) {
            return EqualsBuilder.reflectionEquals(this, o, false);
        }

        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }
    }

}
