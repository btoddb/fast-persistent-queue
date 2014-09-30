package com.btoddb.fastpersitentqueue.eventbus.routers.expressions;

import com.btoddb.fastpersitentqueue.eventbus.FpqEvent;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;


public class StringBodyExpressionTest {

    @Test
    public void testBodyMatcherTrue() {
        StringBodyExpression exp = new StringBodyExpression("=", "is the");

        FpqEvent event = new FpqEvent();
        event.addHeader("foo", "bar")
                .setBodyAsString("this is the body");
        assertThat(exp.match(event), is(true));
    }

    @Test
    public void testBodyMatcherFalse() {
        StringBodyExpression exp = new StringBodyExpression("=", "is the");

        FpqEvent event = new FpqEvent();
        event.addHeader("foo", "bar")
                .setBodyAsString("this is the body");
        assertThat(exp.match(event), is(true));
    }

}