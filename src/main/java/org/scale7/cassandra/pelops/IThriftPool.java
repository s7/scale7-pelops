package org.scale7.cassandra.pelops;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Clock;

/**
 * The contract for connection pools used by pelops.
 */
public interface IThriftPool {
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
     * Create a {@link RowDeletor key deletor} object using the current time as the operation time stamp.
     *
     * @return A new {@link RowDeletor key deletor} object
     */
    RowDeletor createRowDeletor();

    /**
     * Create a {@link RowDeletor key deletor} object with an arbitrary time stamp.
     *
     * @param timestamp The default time stamp to use for operations
     * @return A new {@link RowDeletor key deletor} object
     */
    RowDeletor createRowDeletor(long timestamp);

    /**
     * Get a connection from the pool.
     *
     * @return the connection
     * @throws Exception if an error occurs
     */
    IConnection getConnection() throws Exception;

    /**
     * Get a connection from the pool trying to avoid the node specified by the notNode param.
     *
     * @param notNode the node to avoid if possible
     * @return the connection
     * @throws Exception if an error occurs
     */
    IConnection getConnectionExcept(String notNode) throws Exception;

    /**
     * Shuts down the pool.
     * <p/>
     * Calling this method after the pool has been shutdown should have no affect.
     */
    void shutdown();

    /**
     * Get the current policy in force, which controls the general behavior of pelops.
     *
     * @return the current policy
     */
    OperandPolicy getOperandPolicy();

    /**
     * Create a {@link RowDeletor row deletor} object with an arbitrary time stamp.
     *
     * @param clock The default clock instance to use for operations
     * @return A new {@link RowDeletor row deletor} object
     */
    RowDeletor createRowDeletor(Clock clock);

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
    interface IConnection {
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
         * Returns the id of the session during which this node was connected
         * @return
         */
        int getSessionId();

        /**
         * Close the connection.
         */
        void close();
    }
}
