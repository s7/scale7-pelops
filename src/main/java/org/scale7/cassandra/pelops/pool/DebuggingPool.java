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
