package org.scale7.cassandra.pelops;

import org.apache.cassandra.thrift.Clock;

/**
 * Abstract impl of {@link IThriftPool}.
 */
public abstract class ThriftPoolBase implements IThriftPool {
    /**
     * {@inheritDoc}.
     */
    @Override
    public Selector createSelector() {
        validateKeyspaceSet();
        return new Selector(this);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public Mutator createMutator() {
        validateKeyspaceSet();
        return new Mutator(this);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public Mutator createMutator(long timestamp) {
        return createMutator(timestamp, this.getOperandPolicy().isDeleteIfNull());
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public Mutator createMutator(long timestamp, boolean deleteIfNull) {
        validateKeyspaceSet();
        return new Mutator(this, new Clock(timestamp), deleteIfNull);
    }

    /**
     * {@inheritDoc}.
     */    @Override
    public Mutator createMutator(Clock clock) {
        return createMutator(clock, this.getOperandPolicy().isDeleteIfNull());
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public Mutator createMutator(Clock clock, boolean deleteIfNull) {
        validateKeyspaceSet();
        return new Mutator(this, clock, deleteIfNull);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public RowDeletor createRowDeletor() {
        validateKeyspaceSet();
        return new RowDeletor(this);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public RowDeletor createRowDeletor(long timestamp) {
        validateKeyspaceSet();
        return new RowDeletor(this, new Clock(timestamp));
    }

    /**
     * {@inheritDoc}.
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
