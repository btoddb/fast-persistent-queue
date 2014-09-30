package com.btoddb.fastpersitentqueue.eventbus;


/**
 *
 */
public interface EventBusComponent {
    void init(Config config);
    void shutdown();

    String getId();
    void setId(String id);

    Config getConfig();
    void setConfig(Config config);
}
