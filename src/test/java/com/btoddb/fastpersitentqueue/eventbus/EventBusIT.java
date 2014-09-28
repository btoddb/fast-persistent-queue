package com.btoddb.fastpersitentqueue.eventbus;

/*
 * #%L
 * fast-persistent-queue
 * %%
 * Copyright (C) 2014 btoddb.com
 * %%
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * #L%
 */

import com.btoddb.fastpersitentqueue.config.Config;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;


public class EventBusIT {
    File theDir;
    EventBus bus;
    ObjectMapper objectMapper;

    @Before
    public void setup() throws Exception {
        theDir = new File("junitTmp_"+ UUID.randomUUID().toString());
        FileUtils.forceMkdir(theDir);

        Config config = new Config("conf/config.properties");
        config.setDirectory(theDir.getAbsolutePath());

        bus = new EventBus();
        bus.init(config);
    }

    @After
    public void teardown() throws Exception {
        try {
            bus.shutdown();
        }
        finally {
            FileUtils.deleteDirectory(theDir);
        }

    }

    @Test
    public void testPutSingleEvent() throws Exception {
        FpqBusEvent event = new FpqBusEvent();
        event.addHeader("foo", "bar");
        event.setBody("some-data-for-body");

        Client client = ClientBuilder.newClient();
        Response resp = client.target("http://localhost:8083/v1")
                .request()
                .post(Entity.entity(event, MediaType.APPLICATION_JSON_TYPE));

        assertThat(resp.getStatus(), is(200));
        Map<String, Integer> respObj = resp.readEntity(Map.class);
        assertThat(respObj.get("received"), is(1));

    }

    @Test
    public void testPutEventList() throws Exception {
        int numEvents = 5;
        List<FpqBusEvent> eventList = new ArrayList<FpqBusEvent>(numEvents);
        for (int i=0;i < numEvents;i++) {
            eventList.add(new FpqBusEvent(Collections.singletonMap("msgId", String.valueOf(i)), String.valueOf(i)));
        }

        Client client = ClientBuilder.newClient();
        Response resp = client.target("http://localhost:8083/v1")
                .request()
                .post(Entity.entity(eventList, MediaType.APPLICATION_JSON_TYPE));

        assertThat(resp.getStatus(), is(200));
        Map<String, Integer> respObj = resp.readEntity(Map.class);
        assertThat(respObj.get("received"), is(numEvents));
    }
}