package org.scale7.cassandra.pelops.exceptions;

public class TransportException extends PelopsException {
    public TransportException(Exception e) {
        super(e.getMessage(), e);
    }
}
