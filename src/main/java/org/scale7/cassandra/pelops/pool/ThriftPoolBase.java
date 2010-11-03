package org.scale7.cassandra.pelops.pool;

import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.RowDeletor;
import org.scale7.cassandra.pelops.Selector;
import org.scale7.cassandra.pelops.pool.IThriftPool;


/**
 * Abstract impl of {@link org.scale7.cassandra.pelops.pool.IThriftPool}.
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

    @Override
    public Mutator createMutator(long timestamp, int ttl) {
        return createMutator(timestamp, this.getOperandPolicy().isDeleteIfNull(), ttl);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public Mutator createMutator(long timestamp, boolean deleteIfNull) {
        validateKeyspaceSet();
        return new Mutator(this, timestamp, deleteIfNull);
    }

    @Override
    public Mutator createMutator(long timestamp, boolean deleteIfNull, int ttl) {
        validateKeyspaceSet();
        return new Mutator(this, timestamp, this.getOperandPolicy().isDeleteIfNull(), ttl);
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
        return new RowDeletor(this, timestamp);
    }

    private void validateKeyspaceSet() throws IllegalStateException {
        if (getKeyspace() == null && getKeyspace().isEmpty()) {
            throw new IllegalStateException("A keyspace must be provided in order to use this function.");
        }
    }
}
