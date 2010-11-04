package org.scale7.cassandra.pelops.exceptions;

public class NoConnectionsAvailableException extends PelopsException {
    public NoConnectionsAvailableException() {
    }

    public NoConnectionsAvailableException(String s) {
        super(s);
    }
}
