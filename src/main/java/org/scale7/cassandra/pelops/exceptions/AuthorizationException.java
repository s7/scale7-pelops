package org.scale7.cassandra.pelops.exceptions;

public class AuthorizationException extends PelopsException {
    public AuthorizationException(Exception e) {
        super(e.getMessage(), e);
    }
}
