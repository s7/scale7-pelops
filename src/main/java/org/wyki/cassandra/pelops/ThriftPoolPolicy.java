package org.wyki.cassandra.pelops;

/**
 * Policy object that controls behavior and default values used by a particular Pelops connection
 * pool.
 *
 * @author dominicwilliams
 * @author danwashusen
 */
public abstract class ThriftPoolPolicy {
    private boolean isFramedTransportRequired = false;

    /**
     * Used to determine if the thrift transport should be framed or not.  This is dicated by the 'thrift_framed_transport_size_in_mb'
     * property in cassandra.yaml.
     * @return true if framed transport should be used, otherwise false.
     */
    public boolean isFramedTransportRequired() {
        return isFramedTransportRequired;
    }

    /**
     * Used to determine if the thrift transport should be framed or not.  This is dicated by the 'thrift_framed_transport_size_in_mb'
     * property in cassandra.yaml.
     * @param  framedTransportRequired true if framed transport should be used, otherwise false.
     */
    public void setFramedTransportRequired(boolean framedTransportRequired) {
        isFramedTransportRequired = framedTransportRequired;
    }
}
