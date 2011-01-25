package org.scale7.cassandra.pelops;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;

import java.util.List;

/**
 * Encapsulates the logic required to iterate over columns.  See
 * {@link Selector#iterateColumnsFromRow(String, org.scale7.cassandra.pelops.Bytes, org.scale7.cassandra.pelops.Bytes, boolean, int, org.apache.cassandra.thrift.ConsistencyLevel)}
 * for more detail.
 */
public class ColumnIterator extends PageOfIterator<Column> {
    ColumnIterator(Selector selector, final String columnFamily, final Bytes rowKey,
                               final Bytes startBeyondName, final boolean reversed, final int batchSize,
                               final ConsistencyLevel cLevel) {
        super(selector, columnFamily, rowKey, startBeyondName, reversed, batchSize, cLevel);
    }

    protected List<Column> fetchNextBatch() {
        return this.selector.getPageOfColumnsFromRow(
                this.columnFamily, this.rowKey, this.startBeyondName, this.reversed, this.batchSize, this.cLevel
        );
    }

    @Override
    protected Bytes nextStartBeyondName(List<Column> batch) {
        return batch.isEmpty() ? Bytes.NULL : Bytes.fromByteArray(batch.get(batch.size() - 1).getName());
    }
}
