package com.btoddb.fastpersitentqueue.chronicle.snoopers;

import com.btoddb.fastpersitentqueue.chronicle.FpqEvent;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;


public class TimestampSnooperTest {
    long now = System.currentTimeMillis();

    @Test
    public void testTapMissing() throws Exception {
        TimestampSnooper tap = new TimestampSnooper();
        tap.setHeaderName("the-header");

        FpqEvent event = new FpqEvent("the-body", true)
                .addHeader("foo", "bar");

        tap.tap(event);

        assertThat(Long.parseLong(event.getHeaders().get("the-header")), is(greaterThanOrEqualTo(now)));
    }

    @Test
    public void testTapAlreadyThere() throws Exception {
        TimestampSnooper tap = new TimestampSnooper();
        tap.setHeaderName("the-header");

        FpqEvent event = new FpqEvent("the-body", true)
                .addHeader("foo", "bar")
                .addHeader(tap.getHeaderName(), "123")
                ;

        tap.tap(event);

        assertThat(Long.parseLong(event.getHeaders().get("the-header")), is(123L));
    }

    @Test
    public void testTapAlreadyThereOverwrite() throws Exception {
        TimestampSnooper tap = new TimestampSnooper();
        tap.setHeaderName("the-header");
        tap.setOverwrite(true);

        FpqEvent event = new FpqEvent("the-body", true)
                .addHeader("foo", "bar")
                .addHeader(tap.getHeaderName(), "123")
                ;

        tap.tap(event);

        assertThat(Long.parseLong(event.getHeaders().get("the-header")), is(greaterThanOrEqualTo(now)));
    }
}