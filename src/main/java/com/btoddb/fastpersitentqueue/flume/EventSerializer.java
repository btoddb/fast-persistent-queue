package com.btoddb.fastpersitentqueue.flume;

import com.btoddb.fastpersitentqueue.exceptions.FpqException;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;

import java.util.Map;


/**
 *
 */
public class EventSerializer {
    public static final int VERSION = 1;

    public Event fromBytes(byte[] theBytes) {
        Context context = createContext(theBytes, 0);
        int tmp;

        if (VERSION != (tmp=bytesToInt(context))) {
            throw new FpqException("version, " + tmp + ", not supported");
        }

        Event event = new SimpleEvent();

        tmp = bytesToInt(context);
        for (;tmp > 0;tmp--) {
            event.getHeaders().put(bytesToString(context), bytesToString(context));
        }

        event.setBody(bytesToBytes(context));

        return event;
    }

    public byte[] toBytes(Event event) {
        byte[] theBytes = new byte[calculateBufferLength(event)];
        Context context = createContext(theBytes, 0);

        intToBytes(VERSION, context);

        intToBytes(event.getHeaders().size(), context);
        for (Map.Entry<String, String> header : event.getHeaders().entrySet()) {
            stringToBytes(header.getKey(), context);
            stringToBytes(header.getValue(), context);
        }

        bytesToBytes(event.getBody(), context);

        return context.theBytes;
    }

    int calculateBufferLength(Event event) {
        if (null == event) {
            throw new FpqException("cannot process a null event");
        }

        int sum = calculateVIntBufferSpace(VERSION) + // version
                    calculateVIntBufferSpace(event.getHeaders().size()) // number of headers
                    ;

        for (Map.Entry<String, String> header : event.getHeaders().entrySet()) {
            sum += calculateStringBufferSpace(header.getKey()) + calculateStringBufferSpace(header.getValue());
        }

        return sum + calculateBytesBufferSpace(event.getBody());
    }

    int calculateBytesBufferSpace(byte[] v) {
        if (null != v) {
            return calculateVIntBufferSpace(v.length) + v.length;
        }
        else {
            return calculateVIntBufferSpace(-1);
        }
    }
    void bytesToBytes(byte[] v, Context context) {
        intToBytes(v.length, context);
        System.arraycopy(v, 0, context.theBytes, context.offset, v.length);
        context.offset += v.length;
    }
    byte[] bytesToBytes(Context context) {
        int len = bytesToInt(context);
        byte[] newBytes = new byte[len];
        System.arraycopy(context.theBytes, context.offset, newBytes, 0, len);
        context.offset += len;
        return newBytes;
    }

    int calculateStringBufferSpace(String v) {
        if (null != v) {
            return calculateVIntBufferSpace(v.getBytes().length) + // length of str
                    v.getBytes().length;
        }
        else {
            return calculateVIntBufferSpace(-1);
        }
    }
    void stringToBytes(String v, Context context) {
        bytesToBytes(v.getBytes(), context);
    }
    String bytesToString(Context context) {
        int len = bytesToInt(context);
        String str = new String(context.theBytes, context.offset, len);
        context.offset += len;
        return str;
    }

    int calculateVIntBufferSpace(int v) {
        if (0 == (v & 0xffffff80)) {
            return 1;
        }
        else if (0 != (0xff000000 & v)) {
            return 5;
        }
        else if (0 != (0x00ff0000 & v)) {
            return 4;
        }
        else if (0 != (0x0000ff00 & v)) {
            return 3;
        }
        else {
            return 2;
        }
    }
    void intToBytes(int v, Context context) {
        // if value is less than 128, then just store it in first byte - no length
        if (0 == (v & 0xffffff80)) {
            context.theBytes[context.offset++] = (byte)v;
            return;
        }

        int len = 0;
        while (0 != v) {
            len ++;
            context.theBytes[context.offset+len] = (byte)(v & 0x000000ff);
            v = v >>> 8;
        }
        context.theBytes[context.offset] = (byte)-len;

        context.offset += len+1;
    }
    int bytesToInt(Context context) {
        byte len = context.theBytes[context.offset++];
        if (len >= 0) {
            return len;
        }

        len = (byte) -len;
        int shift = 0;
        int v = 0;
        for (int i=0;i < len;i++) {
            v |= (context.theBytes[context.offset++]&0xff) << shift;
            shift += 8;
        }

        return v;
    }

    public Context createContext(byte[] theBytes, int offset) {
        return new Context(theBytes, offset);
    }

    // -------------

    public class Context {
        byte[] theBytes;
        int offset;
        public Context(byte[] theBytes, int offset) {
            this.theBytes = theBytes;
            this.offset = offset;
        }
    }

}
