package com.btoddb.fastpersitentqueue.eventbus.snoopers;

import com.btoddb.fastpersitentqueue.eventbus.EventBusComponent;
import com.btoddb.fastpersitentqueue.eventbus.FpqEvent;


/**
 *
 */
public interface Snooper extends EventBusComponent {

    /**
     *
     * @param event
     * @return true means keep the event, false means we don't want it anymore and will
     * not be processed by any more Snoopers
     */
    boolean tap(FpqEvent event);

}
