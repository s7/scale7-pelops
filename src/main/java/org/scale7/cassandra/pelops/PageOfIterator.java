package org.scale7.cassandra.pelops;

import org.apache.cassandra.thrift.ConsistencyLevel;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public abstract class PageOfIterator<E> implements Iterator<E> {
    protected final Selector selector;
    protected final String columnFamily;
    protected final Bytes rowKey;
    protected final boolean reversed;
    protected final int batchSize;
    protected final ConsistencyLevel cLevel;
    protected Bytes startBeyondName;
    private boolean isMoreToFetch = false;
    private Iterator<E> currentBatchIterator;

    public PageOfIterator(final Selector selector, final String columnFamily, final Bytes rowKey,
                          final Bytes startBeyondName, final boolean reversed, final int batchSize,
                          final ConsistencyLevel cLevel) {
        this.batchSize = batchSize;
        this.rowKey = rowKey;
        this.cLevel = cLevel;
        this.reversed = reversed;
        this.columnFamily = columnFamily;
        this.startBeyondName = startBeyondName;
        this.selector = selector;
    }

    private void fetchNextBatchInternal() {
        List<E> batch = fetchNextBatch();
        currentBatchIterator = batch.iterator();
        startBeyondName = nextStartBeyondName(batch);
        isMoreToFetch = batch.size() == this.batchSize;
    }

    protected abstract List<E> fetchNextBatch();

    protected abstract Bytes nextStartBeyondName(List<E> batch);

    /**
     * Returns true if the iteration has more super columns. (In other words, returns true if next would return a super column rather than throwing an exception.)
     */
    @Override
    public boolean hasNext() {
        if (currentBatchIterator == null)
            fetchNextBatchInternal();

        boolean isMoreInBatch = currentBatchIterator.hasNext();

        if (!isMoreInBatch && isMoreToFetch) {
            fetchNextBatchInternal();
            return hasNext();
        }

        return isMoreInBatch;
    }

    /**
     * Returns the next super column in the iteration.
     * @return the next super column
     * @throws java.util.NoSuchElementException iteration has no more super columns.
     */
    @Override
    public E next() {
        if (currentBatchIterator == null)
            fetchNextBatchInternal();
        
        E column = null;
        try {
            column = currentBatchIterator.next();
        } catch (NoSuchElementException e) {
            if (isMoreToFetch) {
                fetchNextBatchInternal();
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
