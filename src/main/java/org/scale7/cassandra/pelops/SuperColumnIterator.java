package org.scale7.cassandra.pelops;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SuperColumn;

import java.util.List;

/**
 * Encapsulates the logic required to iterate over super columns.  See
 * {@link org.scale7.cassandra.pelops.Selector#iterateSuperColumnsFromRow(String, Bytes, Bytes, boolean, int, org.apache.cassandra.thrift.ConsistencyLevel)}
 * for more detail.
 */
public class SuperColumnIterator extends PageOfIterator<SuperColumn> {
    public SuperColumnIterator(final Selector selector, final String columnFamily, final Bytes rowKey,
                               final Bytes startBeyondName, final boolean reversed, final int batchSize,
                               final ConsistencyLevel cLevel) {
        super(selector, columnFamily, rowKey, startBeyondName, reversed, batchSize, cLevel);
    }

    protected List<SuperColumn> fetchNextBatch() {
        return this.selector.getPageOfSuperColumnsFromRow(
                this.columnFamily, this.rowKey, this.startBeyondName, this.reversed, this.batchSize, this.cLevel
        );
    }

    @Override
    protected Bytes nextStartBeyondName(List<SuperColumn> batch) {
        return batch.isEmpty() ? null : Bytes.fromBytes(batch.get(batch.size() - 1).getName());
    }
}
