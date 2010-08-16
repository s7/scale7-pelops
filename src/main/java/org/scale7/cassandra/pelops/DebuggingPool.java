package org.scale7.cassandra.pelops;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

import java.net.SocketException;

import static java.lang.String.format;

/**
 * A basic non-pooled pool impl. A new connection is opened each time the {@link #getConnection()} or
 * {@link #getConnectionExcept(String)} is called.
 *
 * This class is useful for diagnostics.
 */
public class DebuggingPool extends ThriftPoolBase {
    private static final Logger logger = SystemProxy.getLoggerFromFactory(DebuggingPool.class);

    Cluster cluster;
    private String keyspace;
    private OperandPolicy generalPolicy;

    public DebuggingPool(Cluster cluster, String keyspace, OperandPolicy generalPolicy) {
        this.cluster = cluster;
        this.keyspace = keyspace;
        this.generalPolicy = generalPolicy;
    }

    @Override
    public IConnection getConnection() throws Exception {
        return new Connection(cluster.getCurrentNodesSnapshot()[0], cluster.getThriftPort(), keyspace);
    }

    @Override
    public IConnection getConnectionExcept(String notNode) throws Exception {
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

    public class Connection implements IConnection {
        private TTransport transport;
        private TProtocol protocol;
        private Cassandra.Client client;
        private String node;
        private int port;
        private String keyspace;

        public Connection(String node, int port, String keyspace) throws SocketException {
            this.node = node;
            this.port = port;
            this.keyspace = keyspace;
        }

        @Override
        public Cassandra.Client getAPI() {
            return client;
        }

        @Override
        public String getNode() {
            return node;
        }

        @Override
        public void flush() throws TTransportException {
            transport.flush();
        }

        @Override
        public void release(boolean afterException) {
            close();
        }

        @Override
        public boolean isOpen() {
            return transport.isOpen();
        }

        @Override
        public boolean open(int nodeSessionId) {
            try {
                TSocket socket = new TSocket(node, port);
                transport = cluster.isFramedTransportRequired() ? new TFramedTransport(socket) : socket;
                protocol = new TBinaryProtocol(transport);
                client = new Cassandra.Client(protocol);

                transport.open();

                if (keyspace != null) {
                    try {
                        client.set_keyspace(keyspace);
                    } catch (Exception e) {
                        throw new IllegalStateException(format("Failed to set keyspace '%s' on client.  See cause for details...", keyspace), e);
                    }
                }
                return true;
            } catch (TTransportException e) {
                logger.error(format("Failed to open transport to %s:%d.  See cause for details...", node, port), e);
                return false;
            }
        }

        @Override
        public void close() {
            transport.close();
        }

		@Override
		public int getSessionId() {
			// TODO Auto-generated method stub
			return 0;
		}
    }
}
