package org.wyki.cassandra.pelops;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.wyki.portability.SystemProxy;

import java.net.SocketException;

import static java.lang.String.format;

/**
 * A basic non-pooled pool impl. A new connection is opened each time the {@link #getConnection()} or
 * {@link #getConnectionExcept(String)} is called.
 *
 * This class is useful for diagnostics.
 */
public class ThriftPoolBasic extends ThriftPoolAbstract {
    private static final Logger logger = SystemProxy.getLoggerFromFactory(ThriftPoolBasic.class);

    private String node;
    private int port;
    private String keyspace;
    private ThriftPoolPolicy poolPolicy;
    private GeneralPolicy generalPolicy;

    public ThriftPoolBasic(String node, int port, String keyspace, ThriftPoolPolicy poolPolicy, GeneralPolicy generalPolicy) {
        this.node = node;
        this.port = port;
        this.keyspace = keyspace;
        this.poolPolicy = poolPolicy;
        this.generalPolicy = generalPolicy;
    }

    @Override
    public Connection getConnection() throws Exception {
        BasicConnection connection = new BasicConnection(node, port, keyspace);
        connection.open(-1);
        return connection;
    }

    @Override
    public Connection getConnectionExcept(String notNode) throws Exception {
        return getConnection();
    }

    @Override
    public void shutdown() {
        // do nothing
    }

    @Override
    public ThriftPoolPolicy getPoolPolicy() {
        return poolPolicy;
    }

    @Override
    public GeneralPolicy getGeneralPolicy() {
        return generalPolicy;
    }

    @Override
    public String getKeyspace() {
        return keyspace;
    }

    public class BasicConnection implements Connection {
        private TTransport transport;
        private TProtocol protocol;
        private Cassandra.Client client;
        private String node;
        private int port;
        private String keyspace;

        public BasicConnection(String node, int port, String keyspace) throws SocketException {
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
//            transport.flush();
        }

        @Override
        public void release(boolean afterException) {
            // do nothing
        }

        @Override
        public boolean isOpen() {
            return transport.isOpen();
        }

        @Override
        public boolean open(int nodeSessionId) {
            try {
                TSocket socket = new TSocket(node, port);
                transport = poolPolicy.isFramedTransportRequired() ? new TFramedTransport(socket) : socket;
                protocol = new TBinaryProtocol(transport);
                client = new Cassandra.Client(protocol);

                transport.open();

                try {
                    client.set_keyspace(keyspace);
                } catch (Exception e) {
                    throw new IllegalStateException(format("Failed to set keyspace '%s' on client.  See cause for details...", keyspace), e);
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
    }
}
