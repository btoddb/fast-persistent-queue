package com.btoddb.fastpersitentqueue.eventbus;

import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;


public class FpqEventTest {
    Config config = new Config();

    @Test
    public void testInstantiateWithBody() {
        FpqEvent event = new FpqEvent("the-body", true);
        assertThat(event.getBody(), is("the-body".getBytes()));
    }

    @Test
    public void testInstantiateWithNuyllBody() {
        FpqEvent event = new FpqEvent((byte[])null);
        assertThat(event.getBody(), is(nullValue()));
        event.getBodyAsString();
    }

    @Test
    public void testGetBodyAsStringString() throws Exception {
        FpqEvent event = new FpqEvent(Collections.singletonMap("foo", "bar"), "a-string", true);
        assertThat(event.getBodyAsString(), is("a-string"));
    }

    @Test
    public void testSerializeWithStringBody() throws Exception {
        FpqEvent event = new FpqEvent(Collections.singletonMap("foo", "bar"), "the-body", true);
        String json = config.getObjectMapper().writeValueAsString(event);
        assertThat(json, is("{\"headers\":{\"foo\":\"bar\"},\"bodyIsText\":true,\"body\":\"the-body\"}"));
    }

    @Test
    public void testDeserializeWithStringBody() throws Exception {
        FpqEvent event = config.getObjectMapper().readValue("{\"headers\":{\"foo\":\"bar\"},\"body\":\"the-body\",\"bodyIsText\":true}", FpqEvent.class);
        assertThat(event.getBodyAsString(), is("the-body"));
        assertThat(event.getHeaders(), hasEntry("foo", "bar"));
    }

    @Test
    public void testSerializeWithBytesBody() throws Exception {
        FpqEvent event = new FpqEvent(Collections.singletonMap("foo", "bar"), new byte[] {0,1,2,3});
        String json = config.getObjectMapper().writeValueAsString(event);
        assertThat(json, is("{\"headers\":{\"foo\":\"bar\"},\"bodyIsText\":false,\"body\":\"AAECAw==\"}"));
    }

    @Test
    public void testDeserializeWithBytesBody() throws Exception {
        FpqEvent event = config.getObjectMapper().readValue("{\"headers\":{\"foo\":\"bar\"},\"body\":\"AAECAw==\",\"bodyIsText\":false}", FpqEvent.class);
        assertThat(event.getBody(), is(new byte[] {0,1,2,3}));
        assertThat(event.getHeaders(), hasEntry("foo", "bar"));
    }
}