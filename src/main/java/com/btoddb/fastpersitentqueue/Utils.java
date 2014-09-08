package com.btoddb.fastpersitentqueue;

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

import com.btoddb.fastpersitentqueue.exceptions.FpqException;
import com.eaio.uuid.UUID;
import org.slf4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;


/**
 * Created by burrb009 on 4/6/14.
 */
public class Utils {


    public static void writeUuidToFile(RandomAccessFile raFile, UUID uuid) throws IOException {
        raFile.writeLong(uuid.getTime());
        raFile.writeLong(uuid.getClockSeqAndNode());
    }

    public static UUID readUuidFromFile(RandomAccessFile raFile) throws IOException {
        return new UUID(raFile.readLong(), raFile.readLong());
    }

    public static void writeInt(RandomAccessFile raFile, int num) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(num);
        raFile.write(bb.array());
    }

    public static int readInt(RandomAccessFile raFile) throws IOException {
        byte[] theBytes = new byte[4];
        int count = raFile.read(theBytes);
        if (4 == count) {
            return ByteBuffer.wrap(theBytes).getInt();
        }
        else {
            throw new EOFException("not enough data to satisfy read of int value");
        }
    }

    public static void writeLong(RandomAccessFile raFile, long num) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(num);
        raFile.write(bb.array());
    }

    public static long readLong(RandomAccessFile raFile) throws IOException {
        byte[] theBytes = new byte[8];
        int count = raFile.read(theBytes);
        if (8 == count) {
            return ByteBuffer.wrap(theBytes).getLong();
        }
        else {
            throw new EOFException("not enough data to satisfy read of long value");
        }
    }

    public static void logAndThrow(Logger logger, String msg) throws FpqException {
        logger.error(msg);
        throw new FpqException(msg);
    }

    public static void logAndThrow(Logger logger, String msg, Exception e) throws FpqException {
        logger.error(msg, e);
        throw new FpqException(msg, e);
    }

}
