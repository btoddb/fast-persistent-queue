package com.btoddb.fastpersitentqueue.flume;

import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.fail;


/**
 *
 */
public class EventSerializerTest {

    @Test
    public void testToFromBytes() throws Exception {
        EventSerializer ser = new EventSerializer();

        Event event1 = new SimpleEvent();
        event1.getHeaders().put("h1", "v1");
        event1.getHeaders().put("h2", "v2");
        event1.setBody("my-body-body".getBytes());

        byte[] theBytes = ser.toBytes(event1);
        Event event2 = ser.fromBytes(theBytes);

        assertThat(event2.getHeaders(), is(event1.getHeaders()));
        assertThat(event2.getBody(), is(event1.getBody()));
    }

    @Test
    public void testEventSize() {
        EventSerializer ser = new EventSerializer();

        Event event = new SimpleEvent();
        event.getHeaders().put("h1", "v1");
        event.setBody("h1=v1".getBytes());

        assertThat(ser.calculateBufferLength(event), is(14));
    }

    @Test
    public void testEmptyEventSize() {
        EventSerializer ser = new EventSerializer();

        Event event = new SimpleEvent();

        assertThat(ser.calculateBufferLength(event), is(3));
    }

    @Test
    public void testBytesToBytesLength() {
        EventSerializer ser = new EventSerializer();
        assertThat(ser.calculateBytesBufferSpace(new byte[0]), is(1));
        assertThat(ser.calculateBytesBufferSpace(new byte[1]), is(2));
        assertThat(ser.calculateBytesBufferSpace(new byte[1000]), is(1003));
        assertThat(ser.calculateBytesBufferSpace(null), is(5));
    }

    @Test
    public void testBytesToBytes() {
        EventSerializer ser = new EventSerializer();

        byte[] theBytes = new byte[1500];
        EventSerializer.Context context = ser.createContext(theBytes, 10);

        byte[] bytes1 = new byte[1000];
        Arrays.fill(bytes1, (byte)123);
        ser.bytesToBytes(bytes1, context);

        context.offset = 10;
        byte[] bytes2 = ser.bytesToBytes(context);
        assertThat(bytes2, is(bytes1));
    }

    @Test
    public void testCalculateStringLength() throws Exception {
        EventSerializer ser = new EventSerializer();

        // uses VInt for string length
        assertThat(ser.calculateStringBufferSpace("0123456789"), is(11));
        assertThat(ser.calculateStringBufferSpace(""), is(1));
        assertThat(ser.calculateStringBufferSpace(null), is(5));
    }

    @Test
    public void testCalculateLengthOfOneByteInt() throws Exception {
        EventSerializer ser = new EventSerializer();
        for (int i=0;i < 127;i++) {
            assertThat(ser.calculateVIntBufferSpace(i), is(1));
        }
        for (int i=128;i < 255;i++) {
            assertThat("i="+i, ser.calculateVIntBufferSpace(i), is(2));
        }
//        System.out.println(String.format("%x", (int)Math.pow(2, 32)+1));
    }

    @Test
    public void testCalculateLengthOfTwoByteInt() throws Exception {
        EventSerializer ser = new EventSerializer();
        assertThat(ser.calculateVIntBufferSpace(256), is(3));
        assertThat(ser.calculateVIntBufferSpace(65535), is(3));
    }

    @Test
    public void testCalculateLengthOfThreeByteInt() throws Exception {
        EventSerializer ser = new EventSerializer();
        assertThat(ser.calculateVIntBufferSpace(65536), is(4));
        assertThat(ser.calculateVIntBufferSpace(16777215), is(4));
    }

    @Test
    public void testCalculateLengthOfFourByteInt() throws Exception {
        EventSerializer ser = new EventSerializer();
        assertThat(ser.calculateVIntBufferSpace(16777216), is(5));
        assertThat(ser.calculateVIntBufferSpace(-1), is(5));
        assertThat(ser.calculateVIntBufferSpace((int) Math.pow(2, 32)), is(5));
        assertThat(ser.calculateVIntBufferSpace((int) Math.pow(2, 32) + 1), is(5));
    }

    @Test
    public void testOneByteIntToBytesAndBack() {
        assertIntConv(0, new byte[10], 2);
        assertIntConv(127, new byte[1], 0);
        assertIntConv(128, new byte[2], 0);
    }

    @Test
    public void testLargerIntToBytesAndBack() {
        assertIntConv(255, new byte[2], 0);
        assertIntConv(1000000, new byte[4], 0);
    }

    @Test
    public void testNegativeIntToBytesAndBack() {
        assertIntConv(-1, new byte[5], 0);
        assertIntConv(-127, new byte[5], 0);
        assertIntConv(-1000000, new byte[5], 0);
    }

    // ---------

    private void assertIntConv(int v1, byte[] theBytes, int offset) {
        EventSerializer ser = new EventSerializer();
        EventSerializer.Context context = ser.createContext(theBytes, offset);
        ser.intToBytes(v1, context);
//        assertThat(context.offset, is(offset+ser.calculateVIntBufferSpace(v1)));
        context.offset = offset;
        assertThat(ser.bytesToInt(context), is(v1));
    }
}
