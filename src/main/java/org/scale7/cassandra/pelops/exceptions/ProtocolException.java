package org.scale7.cassandra.pelops.exceptions;

public class ProtocolException extends PelopsException {
    public ProtocolException(Exception e) {
        super(e.getMessage(), e);
    }
}
