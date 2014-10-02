package com.btoddb.fastpersitentqueue.chronicle.snoopers;

import com.btoddb.fastpersitentqueue.chronicle.ChronicleComponent;
import com.btoddb.fastpersitentqueue.chronicle.FpqEvent;


/**
 *
 */
public interface Snooper extends ChronicleComponent {

    /**
     *
     * @param event
     * @return true means keep the event, false means we don't want it anymore and will
     * not be processed by any more Snoopers
     */
    boolean tap(FpqEvent event);

}
