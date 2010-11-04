package org.scale7.cassandra.pelops.exceptions;

/**
 * Base exception thrown by pelops.
 */
public class PelopsException extends RuntimeException {
    public PelopsException() {
    }

    public PelopsException(String s) {
        super(s);
    }

    public PelopsException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public PelopsException(Throwable throwable) {
        super(throwable);
    }
}
