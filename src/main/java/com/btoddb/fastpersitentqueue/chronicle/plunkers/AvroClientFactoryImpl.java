package com.btoddb.fastpersitentqueue.chronicle.plunkers;

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

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.lang.String.valueOf;
import static org.apache.commons.lang3.StringUtils.substringAfter;
import static org.apache.commons.lang3.StringUtils.substringBefore;
import static org.apache.commons.lang3.math.NumberUtils.toInt;
import static org.apache.flume.api.RpcClientConfigurationConstants.CONFIG_BATCH_SIZE;
import static org.apache.flume.api.RpcClientConfigurationConstants.CONFIG_CLIENT_TYPE;
import static org.apache.flume.api.RpcClientConfigurationConstants.CONFIG_CONNECT_TIMEOUT;
import static org.apache.flume.api.RpcClientConfigurationConstants.CONFIG_HOSTS;
import static org.apache.flume.api.RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX;
import static org.apache.flume.api.RpcClientConfigurationConstants.CONFIG_HOST_SELECTOR;
import static org.apache.flume.api.RpcClientConfigurationConstants.CONFIG_MAX_ATTEMPTS;
import static org.apache.flume.api.RpcClientConfigurationConstants.CONFIG_REQUEST_TIMEOUT;
import static org.apache.flume.api.RpcClientConfigurationConstants.HOST_SELECTOR_ROUND_ROBIN;
import static org.apache.flume.api.RpcClientFactory.ClientType.DEFAULT_FAILOVER;
import static org.apache.flume.api.RpcClientFactory.ClientType.DEFAULT_LOADBALANCE;


/**
 * Used to create RpcClient objects.  The RpcClient is used to communicate with a Flume Avro source.
 */
public class AvroClientFactoryImpl {
    private final Logger logger = LoggerFactory.getLogger(AvroClientFactoryImpl.class);

    public static final int CONNECTION_TIMEOUT = 20000;
    public static final int REQUEST_TIMEOUT = 20000;

    ScheduledExecutorService connResetExecSrvc = Executors.newScheduledThreadPool(
            1,
            new ThreadFactory() {
                @Override
                public Thread newThread(@SuppressWarnings("NullableProblems") Runnable r) {
                    Thread t = new Thread(r);
                    t.setName("DPResetConnection");
                    return t;
                }
            }
    );

    ReentrantReadWriteLock clientLock = new ReentrantReadWriteLock();
    ScheduledFuture resetConnectionFuture;
    volatile RpcClient client;

    private String[] hosts;
    private int connectionsPerHost;
    private int maxBatchSize;
    private boolean configureForFailover;
    private int reconnectPeriod;

    /**
     * Setup factory to create RpcClients described by these parameters.
     *
     * @param hosts                Array of host;port pairs
     * @param connectionsPerHost   number of connections to create per host
     * @param maxBatchSize         max batch size before sending downstream
     * @param configureForFailover configure the RPC client to use failover instead of round-robin
     * @param reconnectPeriod      in seconds - 0 = never reconnect
     */
    public AvroClientFactoryImpl(String[] hosts, int connectionsPerHost, int maxBatchSize, boolean configureForFailover, int reconnectPeriod) {
        this.hosts = hosts;
        this.connectionsPerHost = connectionsPerHost;
        this.maxBatchSize = maxBatchSize;
        this.configureForFailover = configureForFailover;
        this.reconnectPeriod = reconnectPeriod;

        if (0 < reconnectPeriod) {
            this.logger.info("reconnect period = %ds", null, reconnectPeriod);
            initializeConnectionResetThread();
        }
    }

    /**
     * Grabs an instance of RpcClient suitable for the parameters given and sends the batch using it.
     */
    public void getInstanceAndSend(List<Event> batch) throws EventDeliveryException {
        getInstance();
        try {
            clientLock.readLock().lock();
            client.appendBatch(batch);
        }
        finally {
            clientLock.readLock().unlock();
        }
    }

    void getInstance() {
        if (!isNewClientNeeded()) {
            return;
        }
        try {
            clientLock.writeLock().lock();
            if (!isNewClientNeeded()) {
                return;
            }
            if (null != client) {
                client.close();
            }
            client = createRpcClient();
        }
        finally {
            // lock read again so we can unlock it in finally
            clientLock.writeLock().unlock();
        }
    }

    boolean isNewClientNeeded() {
        return null == client || !client.isActive();
    }

    RpcClient createRpcClient() {
        if (1 < hosts.length) {
            return RpcClientFactory.getInstance(createClientProperties(hosts, configureForFailover));
        }

        if (1 < connectionsPerHost) {
            String[] tmpHosts = new String[connectionsPerHost];
            for (int i = 0; i < connectionsPerHost; i++) {
                tmpHosts[i] = hosts[0];
            }
            return RpcClientFactory.getInstance(createClientProperties(tmpHosts, configureForFailover));
        }

        String host = substringBefore(hosts[0], ":");
        int port = toInt(substringAfter(hosts[0], ":"), 4141);

        logger.info("creating client using host = %s:%d", null, host, port);
        return RpcClientFactory.getDefaultInstance(host, port, maxBatchSize);

    }

    Properties createClientProperties(String[] hosts, boolean configureForFailover) {
        Properties properties = new Properties();
        properties.setProperty(CONFIG_BATCH_SIZE, String.valueOf(maxBatchSize));
        properties.setProperty(CONFIG_CONNECT_TIMEOUT, valueOf(CONNECTION_TIMEOUT));
        properties.setProperty(CONFIG_REQUEST_TIMEOUT, valueOf(REQUEST_TIMEOUT));
        if (!configureForFailover) {
            properties.setProperty(CONFIG_CLIENT_TYPE, DEFAULT_LOADBALANCE.name());
            properties.setProperty(CONFIG_HOST_SELECTOR, HOST_SELECTOR_ROUND_ROBIN);
        }
        else {
            properties.setProperty(CONFIG_CLIENT_TYPE, DEFAULT_FAILOVER.name());
            properties.setProperty(CONFIG_MAX_ATTEMPTS, String.valueOf(hosts.length));
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hosts.length; ++i) {
            sb.append(valueOf(i));
            sb.append(' ');
            properties.setProperty(CONFIG_HOSTS_PREFIX + i, hosts[i]);
        }
        properties.setProperty(CONFIG_HOSTS, sb.toString().trim());
        return properties;
    }

    // if configured for failover then reset the connection every reconnectPeriod to switch back
    // to the primary host if happens to failover at some time
    void initializeConnectionResetThread() {
        resetConnectionFuture = connResetExecSrvc.scheduleWithFixedDelay(
                new Runnable() {
                    @Override
                    public void run() {
                        resetConnection();
                    }
                },
                reconnectPeriod, reconnectPeriod, TimeUnit.SECONDS
        );
    }

    void resetConnection() {
        clientLock.writeLock().lock();
        try {
            RpcClient oldClient = client;

            logger.info("reconnecting to distribute load");
            client = createRpcClient();

            if (null != oldClient) {
                logger.debug("closing old client now that we have a new one");
                oldClient.close();
            }
        }
        catch (Exception e) {
            logger.error("exception caught while reconnecting Avro client in reset thread", e);
        }
        finally {
            clientLock.writeLock().unlock();
        }
    }

    public void shutdown() {
        clientLock.writeLock().lock();
        try {
            if (null != connResetExecSrvc) {
                connResetExecSrvc.shutdown();
            }
            if (null != resetConnectionFuture) {
                resetConnectionFuture.cancel(true);
            }
            if (null != client) {
                logger.info("shutting down client");
                client.close();
                client = null;
            }
        }
        finally {
            clientLock.writeLock().unlock();
        }
    }

    @SuppressWarnings("unused")
    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    @SuppressWarnings("unused")
    public String[] getHosts() {
        return hosts;
    }

    @SuppressWarnings("unused")
    public int getConnectionsPerHost() {
        return connectionsPerHost;
    }

    @SuppressWarnings("unused")
    public boolean isConfigureForFailover() {
        return configureForFailover;
    }

    @SuppressWarnings("unused")
    public int getReconnectPeriod() {
        return reconnectPeriod;
    }

}
