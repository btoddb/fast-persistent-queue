package com.btoddb.fastpersitentqueue;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;


/**
 *
 */
public class FpqMemoryMgrTest {
    FpqMemoryMgr mgr;

    @Test
    public void testPushMultipleSegments() throws Exception {
        mgr.init();

        for (int i=0;i < 20;i++) {
            mgr.push(new FpqEntry(new byte[100]));
        }

        assertThat(mgr.getSegments(), hasSize(3));
        assertThat(mgr.getNumberOfEntries(), is(20L));
        assertThat(mgr.getSegments().iterator().next().isAvailableForCleanup(), is(true));
    }

    @Test
    public void testPop() throws Exception {
        mgr.init();

        for (int i=0;i < 20;i++) {
            mgr.push(new FpqEntry(new byte[100]));
        }

        for (int i=0;i < 20;i++) {
            mgr.pop(1);
        }

        assertThat(mgr.getSegments(), hasSize(1));
        assertThat(mgr.getNumberOfEntries(), is(0L));
    }

    // ---------

    @Before
    public void setup() {
        mgr = new FpqMemoryMgr();
        mgr.setMaxSegmentSizeInBytes(1000);
    }

}
