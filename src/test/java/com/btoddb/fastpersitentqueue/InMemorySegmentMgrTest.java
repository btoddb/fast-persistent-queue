package com.btoddb.fastpersitentqueue;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;


/**
 *
 */
public class InMemorySegmentMgrTest {
    long numEntries = 30;
    InMemorySegmentMgr mgr;

    @Test
    public void testPushMultipleSegments() throws Exception {
        mgr.init();

        for (int i=0;i < numEntries;i++) {
            mgr.push(new FpqEntry(new byte[100]));
        }

        assertThat(mgr.getSegments(), hasSize(5));
        assertThat(mgr.getNumberOfEntries(), is(numEntries));
        assertThat(mgr.getSegments().iterator().next().isFull(), is(true));
    }

    @Test
    public void testPop() throws Exception {
        mgr.init();

        for (int i=0;i < numEntries;i++) {
            mgr.push(new FpqEntry(String.format("%02d-3456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",i).getBytes()));
        }

        for (int i=0;i < numEntries;i++) {
            FpqEntry entry = mgr.pop();
            System.out.println(new String(entry.getData()));
        }

        assertThat(mgr.getSegments(), hasSize(1));
        assertThat(mgr.getNumberOfEntries(), is(0L));
    }

    // ---------

    @Before
    public void setup() {
        mgr = new InMemorySegmentMgr();
        mgr.setMaxSegmentSizeInBytes(1000);
    }

}
