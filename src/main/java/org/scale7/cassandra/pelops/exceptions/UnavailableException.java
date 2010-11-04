package org.scale7.cassandra.pelops.exceptions;

public class UnavailableException extends PelopsException {
    public UnavailableException(Exception e) {
        super(e.getMessage(), e);
    }
}
