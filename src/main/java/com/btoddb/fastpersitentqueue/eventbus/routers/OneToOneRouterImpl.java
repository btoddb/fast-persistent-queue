package com.btoddb.fastpersitentqueue.eventbus.routers;

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

import com.btoddb.fastpersitentqueue.eventbus.EventBusComponentBaseImpl;
import com.btoddb.fastpersitentqueue.eventbus.FpqRouter;
import com.btoddb.fastpersitentqueue.eventbus.Config;
import com.btoddb.fastpersitentqueue.eventbus.FpqEvent;
import com.btoddb.fastpersitentqueue.eventbus.PlunkerRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Routes all events received by the named {@link com.btoddb.fastpersitentqueue.eventbus.FpqCatcher} to the
 * specified {@link com.btoddb.fastpersitentqueue.eventbus.FpqPlunker}.
 */
public class OneToOneRouterImpl extends EventBusComponentBaseImpl implements FpqRouter {
    private static final Logger logger = LoggerFactory.getLogger(OneToOneRouterImpl.class);

    private String catcher;
    private String plunker;

    @Override
    public void init(Config config) {
        super.init(config);
    }

    @Override
    public void shutdown() {
        // nothing to do yet
    }

    @Override
    public PlunkerRunner canRoute(String catcherId, FpqEvent event) {
        if (catcher.equals(catcherId)) {
            return config.getPlunkers().get(plunker);
        }
        else {
            return null;
        }
    }

    public String getCatcher() {
        return catcher;
    }

    public void setCatcher(String catcher) {
        this.catcher = catcher;
    }

    public String getPlunker() {
        return plunker;
    }

    public void setPlunker(String plunker) {
        this.plunker = plunker;
    }
}
