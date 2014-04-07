package com.btoddb.fastpersitentqueue;

import com.eaio.uuid.UUID;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.RandomAccessFile;


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

    public static void logAndThrow(Logger logger, String msg) throws FpqException {
        logger.error(msg);
        throw new FpqException(msg);
    }

}
