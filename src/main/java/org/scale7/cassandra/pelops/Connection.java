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

import java.io.IOException;
import java.net.SocketException;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.AsyncClient.login_call;
import org.apache.cassandra.thrift.Cassandra.AsyncClient.set_keyspace_call;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.thrift.TException;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;
import org.scale7.cassandra.pelops.exceptions.IExceptionTranslator;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

public class Connection implements IConnection {
    private static final Logger logger = SystemProxy.getLoggerFromFactory(Connection.class);

    private final Cassandra.AsyncClient client;
    private final Cluster.Node node;
    private final String keyspace;
    private final TNonblockingTransport transport;
    
    public Connection(Cluster.Node node, String keyspace) throws SocketException, TException, InvalidRequestException {
        this.node = node;
        this.keyspace = keyspace;
        
        TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
        try {
            TAsyncClientManager clientManager = new TAsyncClientManager();
            Cassandra.AsyncClient.Factory clientFactory = new Cassandra.AsyncClient.Factory(clientManager, protocolFactory);
            this.transport = new TNonblockingSocket(node.getAddress(), node.getConfig().getThriftPort());
            this.client = clientFactory.getAsyncClient(this.transport);
            if (node.getConfig().isTimeoutSet()) {
                this.client.setTimeout(node.getConfig().getTimeout());
            }
        }
        catch (IOException e) {
            throw new TException(e);
        }
    }

    /**
     * Get a reference to the Cassandra Thrift API
     * @return					The raw Thrift interface
     */
    @Override
    public Cassandra.AsyncClient getAPI() {
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
        
        logger.debug("transport is open '{}'",transport);
        
        if (node.getConfig().getConnectionAuthenticator() !=null) {
        	logger.debug("Authentication request '{}'",node.getConfig().getConnectionAuthenticator());
        	BlockingCallback<login_call> loginHandler = new BlockingCallback<login_call>();
 			try {
 			    getAPI().login(node.getConfig().getConnectionAuthenticator().getAuthenticationRequest(), loginHandler);
				loginHandler.getResult().getResult();
			} catch (Exception e) {
				logger.warn("Failed to login on client for node {}.  Cause is {}", node.getAddress(),  e);
				throw new IExceptionTranslator.ExceptionTranslator().translate(e);
			} 
        }
        
        logger.debug("set keyspace  '{}'",keyspace);

        if (keyspace != null) {
            BlockingCallback<set_keyspace_call> setKeyspaceHandler = new BlockingCallback<set_keyspace_call>();
            try {
                client.set_keyspace(keyspace, setKeyspaceHandler);
                setKeyspaceHandler.getResult().getResult();
            } catch (Exception e) {
                logger.warn("Failed to set keyspace on client for node {}.  Cause is {}", node.getAddress(),  e);
                throw new IExceptionTranslator.ExceptionTranslator().translate(e);
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

