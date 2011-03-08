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

package org.scale7.cassandra.pelops.pool;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.thrift.TException;
import org.scale7.cassandra.pelops.*;
import org.scale7.cassandra.pelops.exceptions.NoConnectionsAvailableException;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

import java.net.SocketException;
import java.util.Random;
import java.util.Set;

import static java.lang.String.format;

/**
 * A basic non-pooled pool impl. A new connection is opened each time the {@link #getConnection()} or
 * {@link IThriftPool#getConnectionExcept(java.util.Set} is called.
 *
 * This class is useful for diagnostics.
 */
public class DebuggingPool extends ThriftPoolBase {
    private static final Logger logger = SystemProxy.getLoggerFromFactory(DebuggingPool.class);

    private Cluster cluster;
    private String keyspace;
    private OperandPolicy generalPolicy;
    private Random random;

    public DebuggingPool(Cluster cluster, String keyspace, OperandPolicy generalPolicy) {
        this.cluster = cluster;
        this.keyspace = keyspace;
        this.generalPolicy = generalPolicy;

        this.random = new Random();
    }

    @Override
    public IPooledConnection getConnection() throws NoConnectionsAvailableException {
        Cluster.Node[] nodes = cluster.getNodes();
        int index = nodes.length == 1 ? 0 : random.nextInt(nodes.length);

        logger.debug("Using node '{}'", nodes[index]);

        PooledConnection connection = null;
        try {
            connection = new PooledConnection(nodes[index], keyspace);
            connection.open();
        } catch (Exception e) {
            throw new NoConnectionsAvailableException();
        }

        return connection;
    }

    @Override
    public IPooledConnection getConnectionExcept(Set<String> avoidNodes) throws NoConnectionsAvailableException {
        return getConnection();
    }

    @Override
    public void shutdown() {
        // Do nothing.. we do not have a handle on number of unreleased connections
    }

    @Override
    public OperandPolicy getOperandPolicy() {
        return generalPolicy;
    }

    @Override
    public String getKeyspace() {
        return keyspace;
    }

    public class PooledConnection extends Connection implements IPooledConnection {
        public PooledConnection(Cluster.Node node, String keyspace) throws SocketException, TException, InvalidRequestException {
            super(node, keyspace);
        }

        @Override
        public void release() {
            close();
        }

        @Override
        public void corrupted() {
            // do nothing (closing anyway)
        }
    }
}
