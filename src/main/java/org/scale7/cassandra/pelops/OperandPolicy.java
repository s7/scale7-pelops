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

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.scale7.cassandra.pelops.exceptions.IExceptionTranslator;

/**
 * General policy configuration.
 */
public class OperandPolicy {
    int maxOpRetries = 3;
    boolean deleteIfNull = false;
    IExceptionTranslator exceptionTranslator = new IExceptionTranslator.ExceptionTranslator();

    public OperandPolicy() {
    }

    public OperandPolicy(int maxOpRetries, boolean deleteIfNull) {
        this.maxOpRetries = maxOpRetries;
        this.deleteIfNull = deleteIfNull;
    }

    public OperandPolicy(int maxOpRetries, boolean deleteIfNull, IExceptionTranslator exceptionTranslator) {
        this.maxOpRetries = maxOpRetries;
        this.deleteIfNull = deleteIfNull;
        this.exceptionTranslator = exceptionTranslator;
    }

    public int getMaxOpRetries() {
        return maxOpRetries;
    }

    /**
     * Max number of times to retry an operation before giving up.
     * Default to 2.
     * @param maxOpRetries the value
     */
    public OperandPolicy setMaxOpRetries(int maxOpRetries) {
        this.maxOpRetries = maxOpRetries;
        return this;
    }

    /**
     * Dictates if pelops should issue deletes when it detects null values being written in a mutation batch.
     * @return true if deletes should be issued by default
     */
    public boolean isDeleteIfNull() {
        return deleteIfNull;
    }

    /**
     * Dictates if pelops should issue deletes when it detects null values being written in a mutation batch.
     * @param deleteIfNull true if deletes should be issued by default
     */
    public OperandPolicy setDeleteIfNull(boolean deleteIfNull) {
        this.deleteIfNull = deleteIfNull;
        return this;
    }

    /**
     * The translater used to convert checked Thirft/Cassandra exceptions into unchecked PelopsExceptions.
     * @return the translator
     */
    public IExceptionTranslator getExceptionTranslator() {
        return exceptionTranslator;
    }

    /**
     * The translater used to convert checked Thirft/Cassandra exceptions into unchecked PelopsExceptions.
     * <p>Note: by default {@link org.scale7.cassandra.pelops.exceptions.IExceptionTranslator.ExceptionTranslator} is used.
     * @param exceptionTranslator the translator
     */
    public OperandPolicy setExceptionTranslator(IExceptionTranslator exceptionTranslator) {
        this.exceptionTranslator = exceptionTranslator;
        return this;
    }

    /**
     * Returns a shallow copy of this object.
     * @return a copy of this
     */
    public OperandPolicy copy() {
        return new OperandPolicy(this.getMaxOpRetries(), this.isDeleteIfNull(), getExceptionTranslator());
    }
}
