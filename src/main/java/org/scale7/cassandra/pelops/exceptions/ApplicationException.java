package org.scale7.cassandra.pelops.exceptions;

import org.apache.thrift.TApplicationException;

public class ApplicationException extends PelopsException {
    public ApplicationException(Exception e) {
        super(e.getMessage(), e);
    }
}
