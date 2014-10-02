package com.btoddb.fastpersitentqueue.eventbus;

import com.btoddb.fastpersitentqueue.eventbus.snoopers.Snooper;
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


public class CatcherWrapperTest {
    EventBus eventBus;

    @Before
    public void setup() {
        eventBus = mock(EventBus.class);

    }

    @Test
    public void testHandleCatcherRemovesEventsProperly() throws Exception {
        List<FpqEvent> eventList = new LinkedList<>();
        eventList.add(new FpqEvent("the-body", true).addHeader("id", "1"));
        eventList.add(new FpqEvent("the-body", true).addHeader("id", "2"));
        eventList.add(new FpqEvent("the-body", true).addHeader("id", "3"));

        Snooper snooper = mock(Snooper.class);
        when(snooper.tap(any(FpqEvent.class))).thenReturn(false, true, false);

        CatcherWrapper wrapper = new CatcherWrapper();
        wrapper.setSnoopers(Collections.singletonMap(snooper.getId(), snooper));
        wrapper.setEventBus(eventBus);

        wrapper.handleCatcher("the-catcher-id", eventList);

        assertThat(eventList, hasSize(1));

        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(eventBus, times(1)).handleCatcher(eq("the-catcher-id"), captor.capture());

        List<FpqEvent> newEventList = captor.getValue();
        assertThat(newEventList, hasSize(1));
        FpqEvent event = (FpqEvent)newEventList.iterator().next();
        assertThat(event.getHeaders().get("id"), is("2"));
    }
}