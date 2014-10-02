package com.btoddb.fastpersitentqueue.chronicle.routers.expressions;

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

import com.btoddb.fastpersitentqueue.exceptions.FpqException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Base implementation for router expressions.
 */
public abstract class ExpressionBaseImpl implements Expression {
    public final Operator op;
    public final String value;
    public final Pattern pattern;

    ExpressionBaseImpl(String op, String value) {
        this.value = value;
        pattern = Pattern.compile(value);
        switch (op) {
            case "=":
                this.op = Operator.EQUAL;
                break;
            case "!=":
                this.op = Operator.NOT_EQUAL;
                break;

            default:
                throw new FpqException("unknown operator, "+op);
        }
    }

    protected boolean internalMatch(String x) {
        Matcher m = pattern.matcher(x);
        switch (op) {
            case EQUAL:
                return m.find();
            case NOT_EQUAL:
                return !m.find();
            default:
                throw new FpqException("operator is invalid : " + op);
        }
    }
}
