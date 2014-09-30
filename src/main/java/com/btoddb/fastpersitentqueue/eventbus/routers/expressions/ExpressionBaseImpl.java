package com.btoddb.fastpersitentqueue.eventbus.routers.expressions;

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
