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

import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;
import org.scale7.cassandra.pelops.exceptions.PelopsException;
import org.scale7.cassandra.pelops.pool.IThriftPool;
import org.scale7.cassandra.pelops.pool.IThriftPool.IPooledConnection;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

import java.util.HashSet;
import java.util.Set;

/**
 * Base class for objects operating against a Cassandra keyspace.
 *
 * @author dominicwilliams
 *
 */
public class Operand {

	private static final Logger logger = SystemProxy.getLoggerFromFactory(Operand.class);

	protected final IThriftPool thrift;

	protected Operand(IThriftPool thrift) {
		this.thrift = thrift;
	}

	protected interface IOperation<ReturnType> {
		ReturnType execute(IPooledConnection conn) throws Exception;
	}

	protected <ReturnType> ReturnType tryOperation(IOperation<ReturnType> operation) throws PelopsException {
        return tryOperation(operation, thrift.getOperandPolicy());
    }

	protected <ReturnType> ReturnType tryOperation(IOperation<ReturnType> operation, OperandPolicy operandPolicy) throws PelopsException {
        Set<String> avoidNodes = null;
		Exception lastException = null;
		int retries = 0;
		do {
			// Get a connection to a Cassandra node
            IPooledConnection conn = null;
            try {
                conn = thrift.getConnectionExcept(avoidNodes);
            } catch (Exception e) {
                // the pool is responsible for blocking and waiting for a connection, so don't retry
                throw operandPolicy.getExceptionTranslator().translate(e);
            }

			try {
				// Execute operation
                // Return result!
				return operation.execute(conn);
			} catch (Exception e) {
                // Should we try again?
                if (e instanceof TimedOutException ||
                    e instanceof TTransportException ||
                    e instanceof UnavailableException) {

                    logger.warn("Operation failed as result of network exception. Connection to node {} is being marked as corrupt " +
                            "(and will probably be be destroyed). Cause of failure is {}", conn.getNode().getAddress(), e);

                    // This connection is "broken" by network timeout or other problem.
                    conn.corrupted();

                    // to avoid create the set for every request create the set here
                    if (avoidNodes == null) avoidNodes = new HashSet<String>(10);
                    avoidNodes.add(conn.getNode().getAddress());

					retries++;
					lastException = e;
				} else if (e instanceof NotFoundException) {
                    // Re-throw application-level exceptions immediately.
					throw operandPolicy.getExceptionTranslator().translate(e);
                } else {
                    // This connection is "broken" by network timeout or other problem.
                    conn.corrupted();

                    // Re-throw application-level exceptions immediately.
					throw operandPolicy.getExceptionTranslator().translate(e);
				}
			} finally {
                conn.release();
            }
		} while (retries < operandPolicy.getMaxOpRetries());

		throw operandPolicy.getExceptionTranslator().translate(lastException);
	}
}
