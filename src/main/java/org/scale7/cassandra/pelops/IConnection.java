/*
 * The MIT License
 *
 * Copyright (c) 2011 Dominic Williams, Daniel Washusen and contributors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package org.scale7.cassandra.pelops;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.transport.TTransportException;

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
     * @throws org.apache.thrift.transport.TTransportException if a thrift error occurs
     */
    void open() throws TTransportException;

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
