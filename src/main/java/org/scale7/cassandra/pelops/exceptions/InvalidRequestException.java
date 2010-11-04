package org.scale7.cassandra.pelops.exceptions;

public class InvalidRequestException extends PelopsException {
    public InvalidRequestException(Exception e) {
        super(e.getMessage(), e);
    }

    /**
     * @see org.apache.cassandra.thrift.InvalidRequestException#getWhy()
     */
    public String getWhy() {
        return ((org.apache.cassandra.thrift.InvalidRequestException) getCause()).getWhy();
    }
}
