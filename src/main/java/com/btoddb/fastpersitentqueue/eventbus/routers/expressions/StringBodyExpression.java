package com.btoddb.fastpersitentqueue.eventbus.routers.expressions;

import com.btoddb.fastpersitentqueue.eventbus.FpqEvent;


/**
 * Tries to match body to regular expression.
 *
 */
public class StringBodyExpression extends ExpressionBaseImpl {
    public StringBodyExpression(String op, String value) {
        super(op, value);
    }


    @Override
    public boolean match(FpqEvent event) {
        return internalMatch(event.getBodyAsString());
    }
}
