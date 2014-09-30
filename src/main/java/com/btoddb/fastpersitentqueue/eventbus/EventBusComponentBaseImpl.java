package com.btoddb.fastpersitentqueue.eventbus;


/**
 * All components in the EventBus architecture should derive from this abstract class.
 */
public abstract class EventBusComponentBaseImpl implements EventBusComponent {
    protected Config config;
    protected String id;

    @Override
    public void init(Config config) {
        this.config = config;
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
        return config;
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }
}
