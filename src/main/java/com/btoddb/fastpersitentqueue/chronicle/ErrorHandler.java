package com.btoddb.fastpersitentqueue.chronicle;

import java.util.Collection;


/**
 *
 */
public interface ErrorHandler {

    void handle(Collection<FpqEvent> events);

}
