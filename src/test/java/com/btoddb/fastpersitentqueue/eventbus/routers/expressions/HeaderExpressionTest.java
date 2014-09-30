package com.btoddb.fastpersitentqueue.eventbus.routers.expressions;

import com.btoddb.fastpersitentqueue.eventbus.FpqEvent;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;


public class HeaderExpressionTest {

    @Test
    public void testHeaderMatcherEqualTrue() {
        HeaderExpression exp = new HeaderExpression("the-header", "=", "val.+");

        FpqEvent event = new FpqEvent();
        event.addHeader("foo", "bar")
                .addHeader("the-header", "target-value");
        assertThat(exp.match(event), is(true));
    }

    @Test
    public void testHeaderMatcherEqualFalse() {
        HeaderExpression exp = new HeaderExpression("the-header", "=", "val.+");

        FpqEvent event = new FpqEvent();
        event.addHeader("foo", "bar")
                .addHeader("the-header", "target-miss");
        assertThat(exp.match(event), is(false));
    }

    @Test
    public void testHeaderMatcherNotEqualTrue() {
        HeaderExpression exp = new HeaderExpression("the-header", "!=", "val.+");

        FpqEvent event = new FpqEvent();
        event.addHeader("foo", "bar")
                .addHeader("the-header", "target");
        assertThat(exp.match(event), is(true));
    }

    @Test
    public void testHeaderMatcherNotEqualFalse() {
        HeaderExpression exp = new HeaderExpression("the-header", "!=", "val.+");

        FpqEvent event = new FpqEvent();
        event.addHeader("foo", "bar")
                .addHeader("the-header", "target-value");
        assertThat(exp.match(event), is(false));
    }

}