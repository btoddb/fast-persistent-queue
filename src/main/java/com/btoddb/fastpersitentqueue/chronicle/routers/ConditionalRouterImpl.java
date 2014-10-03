package com.btoddb.fastpersitentqueue.chronicle.routers;

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
import com.btoddb.fastpersitentqueue.chronicle.ChronicleComponentBaseImpl;
import com.btoddb.fastpersitentqueue.chronicle.FpqEvent;
import com.btoddb.fastpersitentqueue.chronicle.FpqRouter;
import com.btoddb.fastpersitentqueue.chronicle.PlunkerRunner;
import com.btoddb.fastpersitentqueue.chronicle.routers.expressions.AndExpression;
import com.btoddb.fastpersitentqueue.chronicle.routers.expressions.Expression;
import com.btoddb.fastpersitentqueue.chronicle.routers.expressions.HeaderExpression;
import com.btoddb.fastpersitentqueue.chronicle.routers.expressions.OrExpression;
import com.btoddb.fastpersitentqueue.chronicle.routers.expressions.StringBodyExpression;
import com.btoddb.fastpersitentqueue.exceptions.FpqException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Routes all events received by the named {@link com.btoddb.fastpersitentqueue.chronicle.FpqCatcher} to the
 * specified {@link com.btoddb.fastpersitentqueue.chronicle.FpqPlunker}.
 */
public class ConditionalRouterImpl extends ChronicleComponentBaseImpl implements FpqRouter {
    private static final Logger logger = LoggerFactory.getLogger(ConditionalRouterImpl.class);

    private static final Pattern REGEX_AND = Pattern.compile("(.+) +(AND) +(.+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern REGEX_OR = Pattern.compile("(.+) +(OR) +(.+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern REGEX_EXPRESSION = Pattern.compile("([^\\s]+)\\s*([=!]+)\\s*([^\\s]+)");
    private static final Pattern REGEX_HEADER_VAR = Pattern.compile("headers\\[(.+)\\]", Pattern.CASE_INSENSITIVE);

    private String condition;
    private String plunker;

    @Override
    public void init(Config config) throws Exception {
        super.init(config);
        compileExpression(condition);
    }

    Expression compileExpression(String expression) {
        // AND takes precedence, so split over AND first
        Matcher m = REGEX_AND.matcher(expression);
        if (m.find()) {
            return new AndExpression()
                    .addExpression(compileExpression(m.group(1)))
                    .addExpression(compileExpression(m.group(3)));
        }

        // OR is next
        m = REGEX_OR.matcher(expression);
        if (m.find()) {
            return new OrExpression()
                    .addExpression(compileExpression(m.group(1)))
                    .addExpression(compileExpression(m.group(3)));
        }

        // the rest is 'normal' expressions
        Matcher expMatcher = REGEX_EXPRESSION.matcher(expression);
        if (!expMatcher.find()) {
            throw new FpqException("could not 'compile' conditional expression, " + expression);
        }

        String var = expMatcher.group(1);
        if ("body".equalsIgnoreCase(var)) {
            return new StringBodyExpression(expMatcher.group(2), expMatcher.group(3));
        }
        else {
            Matcher varMatcher = REGEX_HEADER_VAR.matcher(var);
            if (varMatcher.find()) {
                return new HeaderExpression(varMatcher.group(1), expMatcher.group(2), expMatcher.group(3));
            }
            else {
                throw new FpqException("cannot compile conditional expression, " + expression);
            }
        }
    }

    @Override
    public void shutdown() {
        // nothing to do yet
    }

    @Override
    public PlunkerRunner canRoute(String catcherId, FpqEvent event) {
        if (calculateIncludes(event)) {
            return config.getPlunkers().get(plunker);
        }
        else {
            return null;
        }
    }

    private boolean calculateIncludes(FpqEvent event) {
        return false;
    }

    public String getPlunker() {
        return plunker;
    }

    public void setPlunker(String plunker) {
        this.plunker = plunker;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }
}

