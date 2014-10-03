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

import com.btoddb.fastpersitentqueue.chronicle.snoopers.Snooper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;


/**
 *
 */
public class RouteAndSnoop {
    private static final Logger logger = LoggerFactory.getLogger(RouteAndSnoop.class);

    private FpqCatcher catcher;
    private Map<String, Snooper> snoopers;

    private Chronicle chronicle;

    public void init(Config config) throws Exception {
        if (null != snoopers) {
            for (Map.Entry<String, Snooper> entry : snoopers.entrySet()) {
                entry.getValue().setId(entry.getKey());
                entry.getValue().init(config);
            }
        }
        else {
            snoopers = Collections.emptyMap();
        }

        // init the catcher last, after all snoopers are ready
        catcher.init(config, this);
    }

    public void shutdown() {
        try {
            catcher.shutdown();
        }
        catch (Exception e) {
            logger.error("exception while shutting down catcher, {}", catcher.getId());
        }

        if (null != snoopers) {
            for (Snooper snooper : snoopers.values()) {
                try {
                    snooper.shutdown();
                }
                catch (Exception e) {
                    logger.error("exception while shutting down snooper, {}", snooper.getId());
                }
            }
        }
    }

    public void handleCatcher(String catcherId, Collection<FpqEvent> eventList) {
        Iterator<FpqEvent> iter = eventList.iterator();
        while (iter.hasNext()) {
            FpqEvent event = iter.next();
            for (Snooper snooper : snoopers.values()) {
                if (!snooper.tap(event)) {
                    // don't want this event anymore, so no more snooping
                    iter.remove();
                    break;
                }
            }
        }
        chronicle.handleCatcher(catcherId, eventList);
    }

    public Chronicle getChronicle() {
        return chronicle;
    }

    public void setChronicle(Chronicle chronicle) {
        this.chronicle = chronicle;
    }

    public FpqCatcher getCatcher() {
        return catcher;
    }

    public void setCatcher(FpqCatcher catcher) {
        this.catcher = catcher;
    }

    public Map<String, Snooper> getSnoopers() {
        return snoopers;
    }

    public void setSnoopers(Map<String, Snooper> snoopers) {
        this.snoopers = snoopers;
    }
}
