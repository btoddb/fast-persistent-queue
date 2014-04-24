package com.btoddb.fastpersitentqueue.exceptions;

/**
 *
 */
public class FpqException extends RuntimeException {
    public FpqException(String msg) {
        super(msg);
    }

    public FpqException(String msg, Exception e) {
        super(msg, e);
    }
}
