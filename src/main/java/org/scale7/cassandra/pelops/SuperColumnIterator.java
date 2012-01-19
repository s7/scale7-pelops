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

import java.util.List;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SuperColumn;

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
        return batch.isEmpty() ? null : Bytes.fromByteArray(batch.get(batch.size() - 1).getName());
    }
}
