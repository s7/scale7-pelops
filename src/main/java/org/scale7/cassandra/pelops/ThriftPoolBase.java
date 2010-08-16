package org.scale7.cassandra.pelops;

import org.apache.cassandra.thrift.Clock;

/**
 * Abstract impl of {@link IThriftPool}.
 */
public abstract class ThriftPoolBase implements IThriftPool {
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
    public RowDeletor createRowDeletor() {
        validateKeyspaceSet();
        return new RowDeletor(this);
    }

    /**
     * Create a <code>KeyDeletor</code> object with an arbitrary time stamp.
     *
     * @param timestamp The default time stamp to use for operations
     * @return A new <code>KeyDeletor</code> object
     */
    @Override
    public RowDeletor createRowDeletor(long timestamp) {
        validateKeyspaceSet();
        return new RowDeletor(this, new Clock(timestamp));
    }

    /**
     * Create a <code>KeyDeletor</code> object with an arbitrary time stamp.
     *
     * @param clock The default clock instance to use for operations
     * @return A new <code>KeyDeletor</code> object
     */
    @Override
    public RowDeletor createRowDeletor(Clock clock) {
        validateKeyspaceSet();
        return new RowDeletor(this, clock);
    }

    private void validateKeyspaceSet() throws IllegalStateException {
        if (getKeyspace() == null && getKeyspace().isEmpty()) {
            throw new IllegalStateException("A keyspace must be provided in order to use this function.");
        }
    }
}
