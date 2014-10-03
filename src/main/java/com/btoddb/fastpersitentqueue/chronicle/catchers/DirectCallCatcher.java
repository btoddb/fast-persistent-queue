package com.btoddb.fastpersitentqueue.chronicle.catchers;

import com.btoddb.fastpersitentqueue.chronicle.FpqEvent;

import java.util.Collection;


/**
 * Created by burrb009 on 10/2/14.
 */
public interface DirectCallCatcher {
    void inject(Collection<FpqEvent> events);
}
