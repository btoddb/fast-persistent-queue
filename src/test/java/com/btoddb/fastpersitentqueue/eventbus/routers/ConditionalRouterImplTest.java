package com.btoddb.fastpersitentqueue.eventbus.routers;

import com.btoddb.fastpersitentqueue.eventbus.routers.expressions.AndExpression;
import com.btoddb.fastpersitentqueue.eventbus.routers.expressions.Expression;
import com.btoddb.fastpersitentqueue.eventbus.routers.expressions.HeaderExpression;
import com.btoddb.fastpersitentqueue.eventbus.routers.expressions.Operator;
import com.btoddb.fastpersitentqueue.eventbus.routers.expressions.OrExpression;
import com.btoddb.fastpersitentqueue.eventbus.routers.expressions.StringBodyExpression;
import org.junit.Test;

import java.util.Iterator;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;


public class ConditionalRouterImplTest {

    @Test
    public void testCompileExpressionHeaderEqual() throws Exception {
        ConditionalRouterImpl router = new ConditionalRouterImpl();
        Expression exp = router.compileExpression("headers[one] = .+");

        assertHeaderExpression(exp, "one", Operator.EQUAL, ".+");
    }

    @Test
    public void testCompileExpressionHeaderNotEqual() throws Exception {
        ConditionalRouterImpl router = new ConditionalRouterImpl();
        Expression exp = router.compileExpression("headers[one] != .+");

        assertHeaderExpression(exp, "one", Operator.NOT_EQUAL, ".+");
    }

    @Test
    public void testAndExpression() {
        // test that headers[one] is set and body != hello
        ConditionalRouterImpl router = new ConditionalRouterImpl();
        Expression exp = router.compileExpression("headers[one] = .+ AND body != hello");

        assertThat(exp, is(instanceOf(AndExpression.class)));

        Iterator<Expression> iter = ((AndExpression) exp).expressionList.iterator();

        assertHeaderExpression(iter.next(), "one", Operator.EQUAL, ".+");
        assertBodyExpression(iter.next(), Operator.NOT_EQUAL, "hello");
    }

    @Test
    public void testOrExpression() {
        // test that headers[one] is set and body != hello
        ConditionalRouterImpl router = new ConditionalRouterImpl();
        Expression exp = router.compileExpression("headers[one] = ^hello OR body = ^hello");

        assertThat(exp, is(instanceOf(OrExpression.class)));

        Iterator<Expression> iter = ((OrExpression) exp).expressionList.iterator();

        assertHeaderExpression(iter.next(), "one", Operator.EQUAL, "^hello");
        assertBodyExpression(iter.next(), Operator.EQUAL, "^hello");
    }

    // --------

    void assertHeaderExpression(Expression exp, String name, Operator op, String regex) {
        assertThat(exp, is(instanceOf(HeaderExpression.class)));
        assertThat(((HeaderExpression)exp).header, is(name));
        assertThat(((HeaderExpression)exp).op, is(op));
        assertThat(((HeaderExpression)exp).value, is(regex));
    }

    void assertBodyExpression(Expression exp, Operator op, String regex) {
        assertThat(exp, is(instanceOf(StringBodyExpression.class)));
        assertThat(((StringBodyExpression)exp).op, is(op));
        assertThat(((StringBodyExpression)exp).value, is(regex));
    }
}