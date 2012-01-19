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

package org.scale7.cassandra.pelops;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;

public abstract class RowIterator<E> implements Iterator<Map.Entry<Bytes, List<E>>> {
    protected final Selector selector;
    protected final String columnFamily;
    protected Bytes startBeyondKey;
    protected final int batchSize;
    protected final SlicePredicate colPredicate;
    protected final ConsistencyLevel cLevel;

    private boolean isFirstBatch = true;
    private boolean isMoreToFetch = false;
    private Iterator<Map.Entry<Bytes, List<E>>> currentBatchIterator;

    protected RowIterator(Selector selector, String columnFamily, Bytes startBeyondKey, int batchSize, SlicePredicate colPredicate, ConsistencyLevel cLevel) {
        this.selector = selector;
        this.columnFamily = columnFamily;
        this.startBeyondKey = startBeyondKey;
        this.batchSize = batchSize;
        this.colPredicate = colPredicate;
        this.cLevel = cLevel;
    }

    private void fetchNextBatchInternal() {
        LinkedHashMap<Bytes, List<E>> batch = fetchNextBatch();
        isMoreToFetch = batch.size() == this.batchSize;

        if (isFirstBatch) {
            isFirstBatch = false;
        } else {
            // we need to remove the first entry in the batch to avoid feeding through the iterator twice...
            batch.remove(this.startBeyondKey);
        }

        currentBatchIterator = batch.entrySet().iterator();

    }

    protected abstract LinkedHashMap<Bytes, List<E>> fetchNextBatch();

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

    @Override
    public Map.Entry<Bytes, List<E>> next() {
        if (currentBatchIterator == null)
            fetchNextBatchInternal();

        Map.Entry<Bytes, List<E>> row = null;
        try {
            row = currentBatchIterator.next();

            if (row != null) {
                startBeyondKey = row.getKey();
            }
        } catch (NoSuchElementException e) {
            if (isMoreToFetch) {
                fetchNextBatchInternal();
                return next();
            }

            throw e;
        }

        return row;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
