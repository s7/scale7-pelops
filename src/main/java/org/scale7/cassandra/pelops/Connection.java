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
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

import java.net.SocketException;

public class Connection implements IConnection {
    private static final Logger logger = SystemProxy.getLoggerFromFactory(Connection.class);

    private Cluster.Node node;
    private String keyspace;

    private TTransport transport;
    private final Cassandra.Client client;

    public Connection(Cluster.Node node, String keyspace) throws SocketException, TException, InvalidRequestException {
        this.node = node;
        this.keyspace = keyspace;

        TSocket socket = new TSocket(node.getAddress(), node.getConfig().getThriftPort());
        transport =  node.getConfig().isFramedTransportRequired() ? new TFramedTransport(socket) : socket;
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        client = new Cassandra.Client(protocol);

        if (node.getConfig().isTimeoutSet())
            socket.setTimeout(node.getConfig().getTimeout());
    }

    /**
     * Get a reference to the Cassandra Thrift API
     * @return					The raw Thrift interface
     */
    @Override
    public Cassandra.Client getAPI() {
        return client;
    }

    /**
     * Get a string identifying the node
     * @return					The IP or DNS address of the node
     */
    @Override
    public Cluster.Node getNode() {
        return node;
    }

    @Override
    public boolean isOpen() {
        return transport.isOpen();
    }

    /**
     * Opens a connection.
     * @return true if the connection was opened, otherwise false
     */
    @Override
    public void open() throws TTransportException {
        if (isOpen()) return;
        
        transport.open();

        if (keyspace != null) {
            try {
                client.set_keyspace(keyspace);
            } catch (Exception e) {
                logger.warn("Failed to set keyspace on client.  See cause for details...", e);
            }
        }
    }

    /**
     * Close the connection.
     */
    @Override
    public void close() {
        transport.close();
    }
}

