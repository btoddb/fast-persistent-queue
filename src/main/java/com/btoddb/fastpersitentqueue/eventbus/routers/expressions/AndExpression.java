package com.btoddb.fastpersitentqueue.eventbus.routers.expressions;

import com.btoddb.fastpersitentqueue.eventbus.FpqEvent;

import java.util.LinkedList;
import java.util.List;


/**
 * 'AND' expressions together.
 */
public class AndExpression implements Expression {
    public List<Expression> expressionList = new LinkedList<>();

    public AndExpression addExpression(Expression expression) {
        expressionList.add(expression);
        return this;
    }

    @Override
    public boolean match(FpqEvent event) {
        for (Expression exp : expressionList) {
            if (exp.match(event)) {
                return true;
            }
        }
        return false;
    }
}
