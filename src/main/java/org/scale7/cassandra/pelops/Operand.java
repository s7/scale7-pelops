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
		String lastNode = null;
		Exception lastException = null;
		int retries = 0;
		do {
			// Get a connection to a Cassandra node
            IPooledConnection conn = null;
            try {
                conn = thrift.getConnectionExcept(lastNode);
            } catch (Exception e) {
                // the pool is responsible for blocking and waiting for a connection, so don't retry
                throw thrift.getOperandPolicy().getExceptionTranslator().translate(e);
            }

            lastNode = conn.getNode().getAddress();
			try {
				// Execute operation
                // Return result!
				return operation.execute(conn);
			} catch (Exception e) {
				// Is this a logic/application error?
				if (e instanceof NotFoundException ||
					e instanceof InvalidRequestException ||
					e instanceof TApplicationException ||
					e instanceof AuthenticationException ||
					e instanceof AuthorizationException) {

                    // Re-throw application-level exceptions immediately.
					throw thrift.getOperandPolicy().getExceptionTranslator().translate(e);
				}
                // Should we try again?
                else if (e instanceof TimedOutException ||
                    e instanceof TTransportException ||
					e instanceof TProtocolException || // maybe this should exit immediately
                    e instanceof UnavailableException) {

                    logger.warn("Operation failed as result of network exception. Connection is being marked as corrupt " +
                            "(and will probably be be destroyed).  See cause for details...", e);

                    // This connection is "broken" by network timeout or other problem.
                    conn.corrupted();

					retries++;
					lastException = e;
				}
                // nope, throw
                else {
                    throw thrift.getOperandPolicy().getExceptionTranslator().translate(e);
                }
			} finally {
                conn.release();
            }
		} while (retries < thrift.getOperandPolicy().getMaxOpRetries());

		throw thrift.getOperandPolicy().getExceptionTranslator().translate(lastException);
	}
}
