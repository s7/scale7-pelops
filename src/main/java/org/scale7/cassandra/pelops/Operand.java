package org.scale7.cassandra.pelops;

import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.transport.TTransportException;
import org.scale7.cassandra.pelops.IThriftPool.IConnection;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

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
		ReturnType execute(IConnection conn) throws Exception;
	}

	protected <ReturnType> ReturnType tryOperation(IOperation<ReturnType> operation) throws Exception {
		String lastNode = null;
		Exception lastException = null;
		int retries = 0;
		do {
			// Get a connection to a Cassandra node
            IConnection conn = thrift.getConnectionExcept(lastNode);
            lastNode = conn.getNode();
			try {
				// Execute operation
				ReturnType result = operation.execute(conn);
				// Release unbroken connection
                conn.release(false);
                // Return result!
				return result;
			} catch (Exception e) {
				// Is this a logic/application error?
				if (e instanceof NotFoundException ||
					e instanceof InvalidRequestException ||
					e instanceof TApplicationException ||
					e instanceof AuthenticationException ||
					e instanceof AuthorizationException) {
					// Logic/application error so we can release unbroken connection
                    conn.release(false);
                    // Re-throw application-level exceptions immediately.
					throw e;
				}
				logger.warn("Operation failed as result of network exception. Connection must be destroyed because of: {} {}", e.getClass(), e.getMessage());
				// This connection is "broken" by network timeout or other problem.
                conn.release(true);
				// Should we try again?
				if (e instanceof TimedOutException ||
                    e instanceof TTransportException ||
                    e instanceof UnavailableException) {
					retries++;
					lastException = e;
				} else // nope, throw
					throw e;
			}
		} while (retries < thrift.getOperandPolicy().getMaxOpRetries());

		throw lastException;
	}
}
