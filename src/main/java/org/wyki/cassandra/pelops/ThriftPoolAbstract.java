package org.wyki.cassandra.pelops;

import org.apache.cassandra.thrift.Clock;

/**
 * Abstract impl of {@link org.wyki.cassandra.pelops.ThriftPool}.
 */
public abstract class ThriftPoolAbstract implements ThriftPool {
    /**
     * Create a <code>Selector</code> object.
     *
     * @return A new <code>Selector</code> object
     */
    @Override
    public Selector createSelector() {
        validateKeyspaceSet();
        return new Selector(this);
    }

    /**
     * Create a <code>Mutator</code> object using the current time as the operation time stamp. The <code>Mutator</code> object
     * must only be used to execute 1 mutation operation.
     *
     * @return A new <code>Mutator</code> object
     */
    @Override
    public Mutator createMutator() {
        validateKeyspaceSet();
        return new Mutator(this);
    }

    /**
     * Create a <code>Mutator</code> object with an arbitrary time stamp. The <code>Mutator</code> object
     * must only be used to execute 1 mutation operation.
     *
     * @param timestamp The default time stamp to use for operations
     * @return A new <code>Mutator</code> object
     */
    @Override
    public Mutator createMutator(long timestamp) {
        validateKeyspaceSet();
        return new Mutator(this, new Clock(timestamp));
    }

    /**
     * Create a <code>Mutator</code> object with an arbitrary time stamp. The <code>Mutator</code> object
     * must only be used to execute 1 mutation operation.
     *
     * @param clock The default clock instance to use for operations
     * @return A new <code>Mutator</code> object
     */
    @Override
    public Mutator createMutator(Clock clock) {
        validateKeyspaceSet();
        return new Mutator(this, clock);
    }

    /**
     * Create a <code>KeyDeletor</code> object using the current time as the operation time stamp.
     *
     * @return A new <code>KeyDeletor</code> object
     */
    @Override
    public KeyDeletor createKeyDeletor() {
        validateKeyspaceSet();
        return new KeyDeletor(this);
    }

    /**
     * Create a <code>KeyDeletor</code> object with an arbitrary time stamp.
     *
     * @param timestamp The default time stamp to use for operations
     * @return A new <code>KeyDeletor</code> object
     */
    @Override
    public KeyDeletor createKeyDeletor(long timestamp) {
        validateKeyspaceSet();
        return new KeyDeletor(this, new Clock(timestamp));
    }

    /**
     * Create a <code>KeyDeletor</code> object with an arbitrary time stamp.
     *
     * @param clock The default clock instance to use for operations
     * @return A new <code>KeyDeletor</code> object
     */
    @Override
    public KeyDeletor createKeyDeletor(Clock clock) {
        validateKeyspaceSet();
        return new KeyDeletor(this, clock);
    }

    /**
     * Create a <code>Metrics</code> object for discovering information about the Cassandra cluster and its contained keyspaces.
     *
     * @return A new <code>Metrics</code> object
     */
    @Override
    public Metrics createMetrics() {
        return new Metrics(this);
    }

    @Override
    public Management createManagement() {
        return new Management(this);
    }

    @Override
    public KeyspaceManagement createKeyspaceManagement() {
        validateKeyspaceSet();
        return new KeyspaceManagement(this);
    }

    private void validateKeyspaceSet() throws IllegalStateException {
        if (getKeyspace() == null && getKeyspace().isEmpty()) {
            throw new IllegalStateException("A keyspace must be provided in order to use this function.");
        }
    }
}
