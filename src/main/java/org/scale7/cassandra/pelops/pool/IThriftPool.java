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

import java.util.Set;

import org.scale7.cassandra.pelops.IConnection;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.OperandPolicy;
import org.scale7.cassandra.pelops.RowDeletor;
import org.scale7.cassandra.pelops.Selector;
import org.scale7.cassandra.pelops.exceptions.NoConnectionsAvailableException;

/**
 * The contract for connection pools used by pelops.
 */
public interface IThriftPool {
    /**
     * Create a {@link org.scale7.cassandra.pelops.Selector selector} object.
     *
     * @return A new {@link org.scale7.cassandra.pelops.Selector selector} object
     */
    Selector createSelector();

    /**
     * Create a {@link org.scale7.cassandra.pelops.Mutator mutator} object using the current time as the operation time stamp.
     * The {@link org.scale7.cassandra.pelops.Mutator mutator} object must only be used to execute 1 mutation operation.
     *
     * @return A new {@link org.scale7.cassandra.pelops.Mutator mutator} object
     */
    Mutator createMutator();

    /**
     * Create a {@link Mutator mutator} object with an arbitrary time stamp. The {@link Mutator mutator} object
     * must only be used to execute 1 mutation operation.
     * <p>The created mutator will be created with the value of {@link org.scale7.cassandra.pelops.OperandPolicy#isDeleteIfNull()}.
     *
     * @param timestamp The default time stamp to use for operations
     * @return A new {@link Mutator mutator} object
     */
    Mutator createMutator(long timestamp);

    /**
     * Create a {@link Mutator mutator} object with an arbitrary time stamp. The {@link Mutator mutator} object
     * must only be used to execute 1 mutation operation.
     * <p>The created mutator will be created with the value of {@link org.scale7.cassandra.pelops.OperandPolicy#isDeleteIfNull()}.
     *
     * @param timestamp The default time stamp to use for operations
     * @param ttl the ttl (in seconds) that columns created using the various {@link Mutator#newColumn(org.scale7.cassandra.pelops.Bytes , org.scale7.cassandra.pelops.Bytes)} will default to
     * @return A new {@link Mutator mutator} object
     */
    Mutator createMutator(long timestamp, int ttl);

    /**
     * Create a {@link Mutator mutator} object with an arbitrary time stamp. The {@link Mutator mutator} object
     * must only be used to execute 1 mutation operation.
     *
     * @param timestamp The default time stamp to use for operations
     * @param deleteIfNull If true the mutator will default to issuing deletes when it detects null values on a column
     *                      passed to the various write methods.
     * @return A new {@link Mutator mutator} object
     */
    Mutator createMutator(long timestamp, boolean deleteIfNull);

    /**
     * Create a {@link Mutator mutator} object with an arbitrary time stamp. The {@link Mutator mutator} object
     * must only be used to execute 1 mutation operation.
     *
     * @param timestamp The default time stamp to use for operations
     * @param deleteIfNull If true the mutator will default to issuing deletes when it detects null values on a column
     *                      passed to the various write methods.
     * @param ttl the ttl (in seconds) that columns created using the various {@link Mutator#newColumn(org.scale7.cassandra.pelops.Bytes , org.scale7.cassandra.pelops.Bytes)} will default to
     * @return A new {@link Mutator mutator} object
     */
    Mutator createMutator(long timestamp, boolean deleteIfNull, int ttl);

    /**
     * Create a {@link org.scale7.cassandra.pelops.RowDeletor key deletor} object using the current time as the operation time stamp.
     *
     * @return A new {@link org.scale7.cassandra.pelops.RowDeletor key deletor} object
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
     * @throws NoConnectionsAvailableException if an error occurs
     */
    IPooledConnection getConnection() throws NoConnectionsAvailableException;

    /**
     * Get a connection from the pool trying to avoid the nodes specified by the avoidNodes param.
     *
     *
     * @param avoidNodes the nodes to avoid if possible (may be null)
     * @return the connection
     * @throws NoConnectionsAvailableException if an error occurs
     */
    IPooledConnection getConnectionExcept(Set<String> avoidNodes) throws NoConnectionsAvailableException;

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
    interface IPooledConnection extends IConnection {
        /**
         * Release a <code>Connection</code> that has previously been taken from the pool.
         */
        void release();

        /**
         * Specify whether an exception has been thrown during usage of the connection. If an exception has been thrown, the
         * connection will not re-used since it may be corrupted (for example, it may contain partially written
         * data that disrupts the serialization of the Thrift protocol) however it is remains essential that all
         * connection objects are released.
         */
        void corrupted();
    }
}
