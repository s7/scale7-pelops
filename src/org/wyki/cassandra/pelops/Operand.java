package org.wyki.cassandra.pelops;

import org.apache.cassandra.thrift.TimedOutException;
import org.apache.thrift.transport.TTransportException;
import org.wyki.cassandra.pelops.ThriftPool.Connection;

/**
 * Base class for objects operating against a Cassandra keyspace.
 * 
 * @author dominicwilliams
 *
 */
public class Operand {
	
	protected final ThriftPool thrift;
	
	protected Operand(ThriftPool thrift) {
		this.thrift = thrift;
	}
	
	protected Object tryOperation(IOperation operation) throws Exception {
		String lastNode = null;
		Exception lastException = null;
		int retries = 0;
		do {
			// Get a connection to a Cassandra node
			Connection conn = thrift.getConnectionExcept(lastNode);
			lastNode = conn.getNode();
			try {
				// Execute operation
				Object result = operation.execute(conn);
				// Release connection
				conn.release(false);
				// Return, we are done!
				return result;
			} catch (Exception e) {
				// Release broken connection
				conn.release(true);
				// Can try continue?
				if (e instanceof TimedOutException ||
					e instanceof TTransportException) {
					retries++;
					lastException = e;
				} else // nope, throw
					throw e;
			}
		} while (retries < thrift.getPolicy().getMaxOpRetries());
		
		throw lastException;
	}
	
}
