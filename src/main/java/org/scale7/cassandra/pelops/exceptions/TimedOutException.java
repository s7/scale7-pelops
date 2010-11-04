package org.scale7.cassandra.pelops.exceptions;

public class TimedOutException extends PelopsException {
    public TimedOutException(Exception e) {
        super(e.getMessage(), e);
    }
}
