package com.btoddb.fastpersitentqueue;

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
