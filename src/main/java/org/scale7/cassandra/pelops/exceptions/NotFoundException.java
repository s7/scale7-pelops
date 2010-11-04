package org.scale7.cassandra.pelops.exceptions;

public class NotFoundException extends PelopsException {
    public NotFoundException(Exception e) {
        super(e.getMessage(), e);
    }
}
