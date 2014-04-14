package com.btoddb.fastpersitentqueue.flume;

import com.btoddb.fastpersitentqueue.Fpq;
import com.btoddb.fastpersitentqueue.FpqContext;
import com.btoddb.fastpersitentqueue.FpqEntry;
import com.btoddb.fastpersitentqueue.Utils;
import org.apache.flume.Event;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;


/**
 *
 */
public class FpqTransaction extends BasicTransactionSemantics {
    private static final Logger logger = LoggerFactory.getLogger(FpqTransaction.class);

    private final Fpq fpq;
    private final FpqContext context;
    private final EventSerializer serializer;

    public FpqTransaction(Fpq fpq, FpqContext context, EventSerializer serializer, int capacity) {
        this.fpq = fpq;
        this.context = context;
        this.serializer = serializer;
    }

    @Override
    protected void doPut(Event event) throws InterruptedException {
        byte[] data = serializer.toBytes(event);
        fpq.push(context, data);
    }

    @Override
    protected Event doTake() throws InterruptedException {
        Collection<FpqEntry> entries = fpq.pop(context, 1);
        if (null == entries || entries.isEmpty()) {
            return null;
        }

        return serializer.fromBytes(entries.iterator().next().getData());
    }

    @Override
    protected void doCommit() throws InterruptedException {
        try {
            fpq.commit(context);
        }
        catch (IOException e) {
            Utils.logAndThrow(logger, "exception while committing transaction");
        }
    }

    @Override
    protected void doRollback() throws InterruptedException {
        fpq.rollback(context);
    }
}
