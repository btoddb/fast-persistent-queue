package com.btoddb.fastpersitentqueue.chronicle;

import com.btoddb.fastpersitentqueue.chronicle.snoopers.Snooper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class RouteAndSnoopTest {
    Chronicle chronicle;

    @Before
    public void setup() {
        chronicle = mock(Chronicle.class);

    }

    @Test
    public void testHandleCatcherRemovesEventsProperly() throws Exception {
        List<FpqEvent> eventList = new LinkedList<>();
        eventList.add(new FpqEvent("the-body", true).addHeader("id", "1"));
        eventList.add(new FpqEvent("the-body", true).addHeader("id", "2"));
        eventList.add(new FpqEvent("the-body", true).addHeader("id", "3"));

        Snooper snooper = mock(Snooper.class);
        when(snooper.tap(any(FpqEvent.class))).thenReturn(false, true, false);

        RouteAndSnoop router = new RouteAndSnoop();
        router.setSnoopers(Collections.singletonMap(snooper.getId(), snooper));
        router.setChronicle(chronicle);

        router.handleCatcher("the-catcher-id", eventList);

        assertThat(eventList, hasSize(1));

        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(chronicle, times(1)).handleCatcher(eq("the-catcher-id"), captor.capture());

        List<FpqEvent> newEventList = captor.getValue();
        assertThat(newEventList, hasSize(1));
        FpqEvent event = newEventList.iterator().next();
        assertThat(event.getHeaders().get("id"), is("2"));
    }
}