package com.btoddb.fastpersitentqueue.eventbus.routers.expressions;

import com.btoddb.fastpersitentqueue.eventbus.FpqEvent;


/**
 * Define interface for expressions.
 */
public interface Expression {
    boolean match(FpqEvent event);
}
