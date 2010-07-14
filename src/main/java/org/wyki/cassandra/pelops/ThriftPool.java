package org.wyki.cassandra.pelops;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Clock;
import org.apache.thrift.transport.TTransportException;

/**
 * The contract for connection pools used by pelops.
 */
public interface ThriftPool {
    /**
     * Create a {@link Selector selector} object.
     *
     * @return A new {@link Selector selector} object
     */
    Selector createSelector();

    /**
     * Create a {@link Mutator mutator} object using the current time as the operation time stamp.
     * The {@link Mutator mutator} object must only be used to execute 1 mutation operation.
     *
     * @return A new {@link Mutator mutator} object
     */
    Mutator createMutator();

    /**
     * Create a {@link Mutator mutator} object with an arbitrary time stamp. The {@link Mutator mutator} object
     * must only be used to execute 1 mutation operation.
     *
     * @param timestamp The default time stamp to use for operations
     * @return A new {@link Mutator mutator} object
     */
    Mutator createMutator(long timestamp);

    /**
     * Create a {@link KeyDeletor key deletor} object using the current time as the operation time stamp.
     *
     * @return A new {@link KeyDeletor key deletor} object
     */
    KeyDeletor createKeyDeletor();

    /**
     * Create a {@link KeyDeletor key deletor} object with an arbitrary time stamp.
     *
     * @param timestamp The default time stamp to use for operations
     * @return A new {@link KeyDeletor key deletor} object
     */
    KeyDeletor createKeyDeletor(long timestamp);

    /**
     * Create a {@link Metrics metrics} object for discovering information about the Cassandra cluster and its contained keyspaces.
     *
     * @return A new {@link Metrics metrics} object
     */
    Metrics createMetrics();

    /**
     * Create a {@link org.wyki.cassandra.pelops.Management} object.
     *
     * @return the instance
     * @see org.wyki.cassandra.pelops.Management
     */
    Management createManagement();

    /**
     * Create a {@link org.wyki.cassandra.pelops.KeyspaceManagement} object.
     *
     * @return the instance
     * @see org.wyki.cassandra.pelops.KeyspaceManagement
     */
    KeyspaceManagement createKeyspaceManagement();

    /**
     * Get a connection from the pool.
     *
     * @return the connection
     * @throws Exception if an error occurs
     */
    Connection getConnection() throws Exception;

    /**
     * Get a connection from the pool trying to avoid the node specified by the notNode param.
     *
     * @param notNode the node to avoid if possible
     * @return the connection
     * @throws Exception if an error occurs
     */
    Connection getConnectionExcept(String notNode) throws Exception;

    /**
     * Shuts down the pool.
     * <p/>
     * Calling this method after the pool has been shutdown should have no affect.
     */
    void shutdown();

    /**
     * Get the current policy in force, which controls the behavioral parameters of the connection pool.
     *
     * @return The current policy
     */
    ThriftPoolPolicy getPoolPolicy();

    /**
     * Get the current policy in force, which controls the general behavior of pelops.
     *
     * @return the current policy
     */
    GeneralPolicy getGeneralPolicy();

    /**
     * Create a <code>KeyDeletor</code> object with an arbitrary time stamp.
     *
     * @param clock The default clock instance to use for operations
     * @return A new <code>KeyDeletor</code> object
     */
    KeyDeletor createKeyDeletor(Clock clock);

    /**
     * Create a <code>Mutator</code> object with an arbitrary time stamp. The <code>Mutator</code> object
     * must only be used to execute 1 mutation operation.
     *
     * @param clock The default clock instance to use for operations
     * @return A new <code>Mutator</code> object
     */
    Mutator createMutator(Clock clock);

    /**
     * The keyspace this connection operates on.
     *
     * @return the keyspace
     */
    String getKeyspace();

    /**
     * Defines an encapsulation for a connection to a Cassandra node.
     *
     * @author dominicwilliams
     * @author danwashusen
     */
    interface Connection {
        /**
         * Get a reference to the Cassandra Thrift API
         *
         * @return The raw Thrift interface
         */
        Cassandra.Client getAPI();

        /**
         * Get a string identifying the node
         *
         * @return The IP or DNS address of the node
         */

        String getNode();

        /**
         * Flush the underlying transport connection used by Thrift. This is used to ensure all
         * writes have been sent to Cassandra.
         *
         * @throws TTransportException
         */
        void flush() throws TTransportException;

        /**
         * Release a <code>Connection</code> that has previously been taken from the pool. Specify whether
         * an exception has been thrown during usage of the connection. If an exception has been thrown, the
         * connection will not re-used since it may be corrupted (for example, it may contain partially written
         * data that disrupts the serialization of the Thrift protocol) however it is remains essential that all
         * connection objects are released.
         *
         * @param afterException Whether a connection was thrown during usage
         */
        void release(boolean afterException);

        /**
         * Used to determine if the connection is open.
         *
         * @return true if the connection is open, otherwise false
         */
        boolean isOpen();

        /**
         * Opens a connection.
         *
         * @param nodeSessionId the node session Id
         * @return true if the connection was opened, otherwise false
         */
        boolean open(int nodeSessionId);

        /**
         * Close the connection.
         */
        void close();
    }
}
