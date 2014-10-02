package com.btoddb.fastpersitentqueue.eventbus.wiretaps;

import com.btoddb.fastpersitentqueue.eventbus.EventBusComponent;
import com.btoddb.fastpersitentqueue.eventbus.FpqEvent;


/**
 *
 */
public interface Snooper extends EventBusComponent {

    void tap(FpqEvent event);

}
