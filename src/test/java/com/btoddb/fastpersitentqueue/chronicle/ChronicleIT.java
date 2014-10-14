package com.btoddb.fastpersitentqueue.chronicle;

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

import com.btoddb.fastpersitentqueue.chronicle.plunkers.TestPlunkerImpl;
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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;


public class ChronicleIT {
    long now = System.currentTimeMillis();
    File theDir;
    Chronicle chronicle;
    Config config;

    @Before
    public void setup() throws Exception {
        theDir = new File("tmp/junitTmp_"+ UUID.randomUUID().toString());
        FileUtils.forceMkdir(theDir);

        config = Config.create("src/test/resources/chronicle-test.yaml");
        for (PlunkerRunner runner : config.getPlunkers().values()) {
            runner.getFpq().setJournalDirectory(new File(theDir, runner.getPlunker().getId()+"/journals"));
            runner.getFpq().setPagingDirectory(new File(theDir, runner.getPlunker().getId()+"/pages"));
        }

        chronicle = new Chronicle();
        chronicle.init(config);
    }

    @After
    public void teardown() throws Exception {
        try {
            chronicle.shutdown();
        }
        finally {
            FileUtils.deleteDirectory(theDir);
        }
    }

    @Test
    public void testPutSingleEvent() throws Exception {
        FpqEvent event = new FpqEvent("some-data-for-body")
                .withHeader("foo", "bar");

        Client client = ClientBuilder.newClient();
        Response resp = client.target("http://localhost:8083/v1")
                .request()
                .post(Entity.entity(event, MediaType.APPLICATION_JSON_TYPE));

        assertThat(resp.getStatus(), is(200));

        Map<String, Integer> respObj = resp.readEntity(Map.class);
        assertThat(respObj.get("received"), is(1));

        List<FpqEvent> eventList = retrieveChronicleEvents(1);
        assertThat(eventList, hasSize(1));
        assertThat(Long.parseLong(eventList.get(0).getHeaders().get("timestamp")), is(greaterThanOrEqualTo(now)));
        eventList.get(0).getHeaders().remove("timestamp");
        assertThat(eventList.get(0), is(event));
    }

    @Test
    public void testPutEventList() throws Exception {
        int numEvents = 5;
        List<FpqEvent> eventList = new ArrayList<>(numEvents);
        for (int i=0;i < numEvents;i++) {
            eventList.add(new FpqEvent(Collections.singletonMap("msgId", String.valueOf(i)), String.valueOf(i), true));
        }

        Client client = ClientBuilder.newClient();
        Response resp = client.target("http://localhost:8083/v1")
                .request()
                .post(Entity.entity(eventList, MediaType.APPLICATION_JSON_TYPE));

        assertThat(resp.getStatus(), is(200));
        Map<String, Integer> respObj = resp.readEntity(Map.class);
        assertThat(respObj.get("received"), is(numEvents));

        List<FpqEvent> plunkEventList = retrieveChronicleEvents(numEvents);
        assertThat(plunkEventList, hasSize(numEvents));
        for (FpqEvent evt : plunkEventList) {
            assertThat(Long.parseLong(evt.getHeaders().get("timestamp")), is(greaterThanOrEqualTo(now)));
            evt.getHeaders().remove("timestamp");
        }
        assertThat(plunkEventList, containsInAnyOrder(eventList.toArray()));
    }

    // ----------

    private List<FpqEvent> retrieveChronicleEvents(int numEvents) {
        return ((TestPlunkerImpl) chronicle.getConfig().getPlunkers().values().iterator().next().getPlunker()).waitForEvents(numEvents, 5000);
    }

}