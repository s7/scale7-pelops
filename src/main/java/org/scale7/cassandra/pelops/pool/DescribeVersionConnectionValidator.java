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

import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

/**
 * A simple impl. that issues a call to {@link org.apache.cassandra.thrift.Cassandra.Client#describe_version} to
 * validate that the connection still works.
 */
public class DescribeVersionConnectionValidator implements CommonsBackedPool.IConnectionValidator {
    private static final Logger logger = SystemProxy.getLoggerFromFactory(DescribeVersionConnectionValidator.class);
    /**
     * Returns true if the call to {@link org.apache.cassandra.thrift.Cassandra.Client#describe_version} succeeds.
     * @param connection the connection
     * @return true if the connection is valid, otherwise false
     */
    @Override
    public boolean validate(CommonsBackedPool.PooledConnection connection) {
        try {
            connection.getAPI().describe_version();
        } catch (Exception e) {
            logger.debug("Connection '{}' failed to validate", connection);
            return false;
        }

        logger.debug("Connection '{}' passed validation", connection);
        return true;
    }
}
