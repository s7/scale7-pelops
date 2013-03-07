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

import java.net.SocketException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.scale7.cassandra.pelops.exceptions.IExceptionTranslator;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

public class Connection implements IConnection {
    private static final Logger logger = SystemProxy.getLoggerFromFactory(Connection.class);
    
    public static String[] DEFAULT_CIPHER_SUITES = {"TLS_RSA_WITH_AES_128_CBC_SHA"/*, "TLS_RSA_WITH_AES_256_CBC_SHA"*/};
    
    public static String DEFAULT_PROTOCOL = "TLS";

    public static final int DEFAULT_MAX_FRAME_SIZE = 15 * 1024 * 1024; // 15 MiB
    
    private Cluster.Node node;
    private String keyspace;
    private TTransport transport;
    private final Cassandra.Client client;
    
    private boolean connectionInitialized;
    
    public Connection(Cluster.Node node, String keyspace) throws SocketException, TException, InvalidRequestException {
        this.node = node;
        this.keyspace = keyspace;
        this.connectionInitialized = false;

        if(node.getConfig().isSSLTransportRequired()) {
            TSSLTransportFactory.TSSLTransportParameters params =
                    new TSSLTransportFactory.TSSLTransportParameters(DEFAULT_PROTOCOL, DEFAULT_CIPHER_SUITES);
            params.setTrustStore(node.getConfig().getTrustStorePath(),
                                 node.getConfig().getTrustStorePassword());
            
            //This opens the socket
            TSocket clientSocket = TSSLTransportFactory.getClientSocket(
                    node.getAddress(), node.getConfig().getThriftPort(), node.getConfig().getTimeout(), params);
            
            //SSL requires framed transport
            transport = new TFramedTransport(clientSocket, DEFAULT_MAX_FRAME_SIZE);
            
            logger.debug("transport is open '{}'",transport);
        } else {
            TSocket socket = new TSocket(node.getAddress(), node.getConfig().getThriftPort());
        
            transport =  node.getConfig().isFramedTransportRequired()
                                    ? new TFramedTransport(socket) : socket;

            if (node.getConfig().isTimeoutSet())
                socket.setTimeout(node.getConfig().getTimeout());
        }
        
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        client = new Cassandra.Client(protocol);
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
     */
    @Override
    public void open() throws TTransportException {
        if (isOpen() && connectionInitialized == true) return;
        
        if(isOpen() == false) {
            transport.open();
            logger.debug("transport is open '{}'",transport);
        }
        
        if (node.getConfig().getConnectionAuthenticator() !=null) {
                logger.debug("Authentication request '{}'",node.getConfig().getConnectionAuthenticator());
                        try {
                                getAPI().login(node.getConfig().getConnectionAuthenticator().getAuthenticationRequest());
                        } catch (Exception e) {
                                logger.warn("Failed to login on client for node {}.  Cause is {}", node.getAddress(),  e);
                                throw new IExceptionTranslator.ExceptionTranslator().translate(e);
                        } 
        }

        logger.debug("set keyspace  '{}'",keyspace);

        if (keyspace != null) {
            try {
                client.set_keyspace(keyspace);
            } catch (Exception e) {
                logger.warn("Failed to set keyspace on client for node {}.  Cause is {}", node.getAddress(),  e);
                throw new IExceptionTranslator.ExceptionTranslator().translate(e);
            }
        }
        
        //This is used due to the SSL socket being opened by the TSSLTransportFactory
        connectionInitialized = true;
    }

    /**
     * Close the connection.
     */
    @Override
    public void close() {
        transport.close();
    }
}

