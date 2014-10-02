package com.btoddb.fastpersitentqueue.chronicle.snoopers;

import com.btoddb.fastpersitentqueue.chronicle.Config;
import com.btoddb.fastpersitentqueue.chronicle.FpqEvent;


/**
 * Adds timestamp to event.
 */
public class TimestampSnooper implements Snooper {
    private String id;
    private String headerName;
    private boolean overwrite;


    @Override
    public void init(Config config) {
        // do nothing
    }

    @Override
    public void shutdown() {
        // do nothing
    }

    @Override
    public boolean tap(FpqEvent event) {
        if (overwrite || !event.getHeaders().containsKey(headerName)) {
            event.addHeader(headerName, String.valueOf(System.currentTimeMillis()));
        }

        return true;
    }

    public String getHeaderName() {
        return headerName;
    }

    public void setHeaderName(String headerName) {
        this.headerName = headerName;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public void setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    @Override
    public Config getConfig() {
        return null;
    }

    @Override
    public void setConfig(Config config) {
        // don't care
    }
}
