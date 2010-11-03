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
    public boolean open() {
        if (isOpen()) return true;
        
        try {
            transport.open();

            if (keyspace != null) {
                try {
                    client.set_keyspace(keyspace);
                } catch (Exception e) {
                    logger.warn("Failed to set keyspace on client.  See cause for details...", e);
                    return false;
                }
            }
        } catch (TTransportException e) {
            logger.error("Failed to open transport.  See cause for details...", e);
            return false;
        }
        return true;
    }

    /**
     * Close the connection.
     */
    @Override
    public void close() {
        transport.close();
    }
}

