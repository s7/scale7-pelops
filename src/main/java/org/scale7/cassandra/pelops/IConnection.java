package org.scale7.cassandra.pelops;

import org.apache.cassandra.thrift.Cassandra;

public interface IConnection {
    /**
     * Get a reference to the Cassandra Thrift API
     *
     * @return The raw Thrift interface
     */
    Cassandra.Client getAPI();

    /**
     * Get a node this instance is connected to.
     *
     * @return the node
     */

    Cluster.Node getNode();

    /**
     * Used to determine if the connection is open.
     *
     * @return true if the connection is open, otherwise false
     */
    boolean isOpen();

    /**
     * Opens a connection.
     *
     * @return true if the connection was opened, otherwise false
     */
    boolean open();

    /**
     * Close the connection.
     */
    void close();

    public static class Config {
        private final int thriftPort;
        private final boolean framedTransportRequired;
        private final int timeout;

        /**
         * Config used when opening connections.
         * @param thriftPort the port to connect to
         * @param framedTransportRequired is framed transport required
         * @param timeout the timeout (0 or less to use thrift default)
         */
        public Config(int thriftPort, boolean framedTransportRequired, int timeout) {
            this.thriftPort = thriftPort;
            this.framedTransportRequired = framedTransportRequired;
            this.timeout = timeout;
        }

        public int getThriftPort() {
            return thriftPort;
        }

        public boolean isFramedTransportRequired() {
            return framedTransportRequired;
        }

        public int getTimeout() {
            return timeout;
        }

        /**
         * Determines if a timeout should be set on the thrift connection.
         * @return true if the timeout value is greater than zero, false otherwise
         */
        public boolean isTimeoutSet() {
            return getTimeout() > 0;
        }
    }
}
