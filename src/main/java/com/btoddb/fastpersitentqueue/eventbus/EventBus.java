package com.btoddb.fastpersitentqueue.eventbus;

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

import com.btoddb.fastpersitentqueue.Fpq;
import com.btoddb.fastpersitentqueue.Utils;
import com.btoddb.fastpersitentqueue.config.Config;
import com.btoddb.fastpersitentqueue.exceptions.FpqException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
public class EventBus {
    private static final Logger logger = LoggerFactory.getLogger(EventBus.class);

    private Fpq fpq;
    private RestEndpointImpl endpoint;

    public void init(Config config) throws FpqException {
        fpq = new Fpq();
        try {
            fpq.init(config);
        }
        catch (Exception e) {
            Utils.logAndThrow(logger, "exception while initializing FPQ - cannot continue", e);
        }

        endpoint = new RestEndpointImpl();
        endpoint.init(fpq, config);
    }

    public void shutdown() {
        try {
            fpq.shutdown();
        }
        catch (Exception e) {
            logger.error("exception while shutting down FPQ", e);
        }

        try {
            endpoint.shutdown();
        }
        catch (Exception e) {
            logger.error("exception while shutting down FPQ", e);
        }
    }
}
