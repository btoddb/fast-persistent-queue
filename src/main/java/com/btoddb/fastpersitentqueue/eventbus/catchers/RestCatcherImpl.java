package com.btoddb.fastpersitentqueue.eventbus.catchers;

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
import com.btoddb.fastpersitentqueue.eventbus.FpqCatcher;
import com.btoddb.fastpersitentqueue.eventbus.FpqEvent;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.jmx.MBeanContainer;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;


public class RestCatcherImpl implements FpqCatcher {
    private static final Logger logger = LoggerFactory.getLogger(RestCatcherImpl.class);

    private String id;
    private int port = 8083;
    private String bind = "0.0.0.0";
    private int maxBatchSize = 100;
    private Fpq fpq;

    Server server;
    ObjectMapper objectMapper;

    @Override
    public void init(Fpq fpq) {
        this.fpq = fpq;
        objectMapper = new ObjectMapper();

        startJettyServer();
    }

    private void startJettyServer() {
        QueuedThreadPool tp = new QueuedThreadPool(200, 8, 30000, new ArrayBlockingQueue<Runnable>(1000));
        tp.setName("EventBus-RestV1-ThreadPool");

        server = new Server(tp);

        // Setup JMX - do this before setting *anything* else on the server object
        MBeanContainer mbContainer = new MBeanContainer(ManagementFactory.getPlatformMBeanServer());
        server.addEventListener(mbContainer);
        server.addBean(mbContainer);
        server.addBean(Log.getLogger(RestCatcherImpl.class));

        ServerConnector connector = new ServerConnector(server);
        connector.setHost(bind);
        connector.setPort(port);

        HandlerCollection handlers = new HandlerCollection();
        handlers.addHandler(new RequestHandler());
        handlers.addHandler(new DefaultHandler());

        StatisticsHandler statsHandler = new StatisticsHandler();
        statsHandler.setHandler(handlers);

        ContextHandler context = new ContextHandler();
        context.setDisplayName("EventBus-RestV1");
        context.setContextPath("/v1");
        context.setHandler(statsHandler);
        context.setAllowNullPathInfo(true); // to avoid redirect on POST


        server.setConnectors(new Connector[] {connector});
        server.setHandler(context);

        try {
            server.start();
            logger.info("jetty started and listening on port " + port);
        }
        catch (Exception e) {
            logger.error("exception while starting jetty server", e);
        }
    }

    public boolean isJsonArray(InputStream inStream) {
        if (null == inStream) {
            return false;
        }

        int count = 100;
        inStream.mark(count);
        int ch;
        try {
            do {
                ch = inStream.read();
            } while ('[' != ch && '{' != ch && Character.isWhitespace((char)ch) && -1 != ch && --count > 0);

            inStream.reset();
            if (0 == count) {
                Utils.logAndThrow(logger, "unrecognizable JSON, or too much whitespace before JSON doc starts");
            }
            return '[' == ch;
        }
        catch (IOException e) {
            Utils.logAndThrow(logger, "exception while looking for JSON in InputStream", e);
        }

        return false;
    }

    @Override
    public void shutdown() throws Exception {
        server.stop();
    }

    public class RequestHandler extends AbstractHandler {
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
                throws IOException, ServletException {
            // make sure we have a POST (PUT has the assumption to be idempotent - FPQ is not)
            if (!"POST".equals(request.getMethod())) {
                response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
                response.getWriter().print("For this request the server only supports POST - in particular PUT is not supported because FPQ is not idempotent");
                return;
            }

            //
            // TODO:BTB - check that request isn't "too large"
            //

            List<FpqEvent> eventList;
            BufferedInputStream reqInStream = new BufferedInputStream(request.getInputStream());

            // check for list of events, or single event
            if (!isJsonArray(reqInStream)) {
                FpqEvent event = objectMapper.readValue(reqInStream, new TypeReference<FpqEvent>() {});
                eventList = Collections.singletonList(event);
            }
            else {
                try {
                    eventList = objectMapper.readValue(reqInStream, new TypeReference<List<FpqEvent>>() {});
                }
                catch (Exception e) {
                    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                    response.getWriter().print(e.getMessage());
                    return;
                }
            }

            // parse list of events

            if (eventList.size() > maxBatchSize) {
                response.setStatus(HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE);
                response.getWriter().print(String.format("Too many events received. The maximum batch size is %d", maxBatchSize));
                return;
            }

            try {
                fpq.beginTransaction();
                for (FpqEvent event : eventList) {
                    fpq.push(objectMapper.writeValueAsBytes(event));
                }
                fpq.commit();
            }
            catch (Exception e) {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                response.getWriter().print(e.getMessage());
                return;
            }
            finally {
                if (fpq.isTransactionActive()) {
                    fpq.rollback();
                }
            }

            baseRequest.setHandled(true);

            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("application/json");
            response.getWriter().print(String.format("{ \"received\": %d}", eventList.size()));
        }
    }

    @SuppressWarnings("unused")
    public void setBind(String bind) {
        this.bind = bind;
    }

    @SuppressWarnings("unused")
    public void setPort(int port) {
        this.port = port;
    }

    public int getPort() {
        return port;
    }

    public String getBind() {
        return bind;
    }

    @SuppressWarnings("unused")
    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    @SuppressWarnings("unused")
    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }


    @Override
    @SuppressWarnings("unused")
    public Fpq getFpq() {
        return fpq;
    }

    @Override
    @SuppressWarnings("unused")
    public void setFpq(Fpq fpq) {
        this.fpq = fpq;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }
}
