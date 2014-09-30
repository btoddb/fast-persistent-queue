package com.btoddb.fastpersitentqueue.eventbus.routers.expressions;

import com.btoddb.fastpersitentqueue.eventbus.FpqEvent;


/**
 * Created by burrb009 on 9/30/14.
 */
public class HeaderExpression extends ExpressionBaseImpl {
    public final String header;

    public HeaderExpression(String header, String op, String value) {
        super(op, value);
        this.header = header;
    }


    @Override
    public boolean match(FpqEvent event) {
        return internalMatch(event.getHeaders().get(header));
    }
}
