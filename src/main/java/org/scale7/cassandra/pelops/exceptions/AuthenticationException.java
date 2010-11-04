package org.scale7.cassandra.pelops.exceptions;

public class AuthenticationException extends PelopsException {
    public AuthenticationException(Exception e) {
        super(e.getMessage(), e);
    }
}
