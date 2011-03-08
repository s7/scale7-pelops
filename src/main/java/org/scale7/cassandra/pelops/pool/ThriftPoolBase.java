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
