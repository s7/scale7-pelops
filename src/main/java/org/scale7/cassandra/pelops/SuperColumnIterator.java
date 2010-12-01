package org.scale7.cassandra.pelops;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SuperColumn;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Encapsulates the logic required to iterate over super columns.  See
 * {@link org.scale7.cassandra.pelops.Selector#iterateSuperColumnsFromRow(String, Bytes, Bytes, boolean, int, org.apache.cassandra.thrift.ConsistencyLevel)}
 * for more detail.
 */
public class SuperColumnIterator implements Iterator<SuperColumn> {
    private final Selector selector;

    private final String columnFamily;
    private final Bytes rowKey;
    private final boolean reversed;
    private final int batchSize;
    private final ConsistencyLevel cLevel;

    private Bytes startBeyondName;
    private boolean isMoreToFetch = false;
    private Iterator<SuperColumn> currentBatchIterator;

    SuperColumnIterator(Selector selector, final String columnFamily, final Bytes rowKey,
                               final Bytes startBeyondName, final boolean reversed, final int batchSize,
                               final ConsistencyLevel cLevel) {
        this.selector = selector;
        this.columnFamily = columnFamily;
        this.rowKey = rowKey;
        this.startBeyondName = startBeyondName;
        this.reversed = reversed;
        this.batchSize = batchSize;
        this.cLevel = cLevel;

        fetchNextBatch();
    }

    private void fetchNextBatch() {
        List<SuperColumn> batch = this.selector.getPageOfSuperColumnsFromRow(
                this.columnFamily, this.rowKey, this.startBeyondName, this.reversed, this.batchSize, this.cLevel
        );
        currentBatchIterator = batch.iterator();
        startBeyondName = batch.isEmpty() ? null : Bytes.fromBytes(batch.get(batch.size() - 1).getName());
        isMoreToFetch = batch.size() == this.batchSize;
    }

    /**
     * Returns true if the iteration has more super columns. (In other words, returns true if next would return a super column rather than throwing an exception.)
     */
    @Override
    public boolean hasNext() {
        boolean isMoreInBatch = currentBatchIterator.hasNext();

        if (!isMoreInBatch && isMoreToFetch) {
            fetchNextBatch();
            return hasNext();
        }

        return isMoreInBatch;
    }

    /**
     * Returns the next super column in the iteration.
     * @return the next super column
     * @throws NoSuchElementException iteration has no more super columns.
     */
    @Override
    public SuperColumn next() {
        SuperColumn column = null;
        try {
            column = currentBatchIterator.next();
        } catch (NoSuchElementException e) {
            if (isMoreToFetch) {
                fetchNextBatch();
                return next();
            }

            throw e;
        }
        
        return column;
    }

    /**
     * Not supported.
     */
    @Override
    public void remove() {
        throw new IllegalArgumentException("You can't remove columns using the iterator.");
    }
}
