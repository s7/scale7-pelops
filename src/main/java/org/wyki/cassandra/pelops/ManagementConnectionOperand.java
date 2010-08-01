package org.wyki.cassandra.pelops;

import org.apache.cassandra.thrift.*;
import org.apache.thrift.TApplicationException;

public class ManagementConnectionOperand extends Operand {
    protected ThriftPool.Connection conn;

    public ManagementConnectionOperand(ThriftPool thrift) {
        super(thrift);
        try {
            conn = thrift.getManagementConnection();
            conn.open(-1);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected <ReturnType> ReturnType tryOperation(IOperation<ReturnType> operation) throws Exception {
        return tryOperation(conn, operation);
    }

    private <ReturnType> ReturnType tryOperation(ThriftPool.Connection conn, IOperation<ReturnType> operation) throws Exception {
        Exception lastException = null;
        int retries = 0;
        do {
            try {
                // Execute operation
                // Return result!
                ReturnType result = operation.execute(conn);
                return result;
            } catch (Exception e) {
                // Is this an error we can't work around?
                if (e instanceof NotFoundException ||
                        e instanceof InvalidRequestException ||
                        e instanceof TApplicationException ||
                        e instanceof AuthenticationException ||
                        e instanceof AuthorizationException) {
                    // Re-throw application-level exceptions immediately.
                    throw e;
                }
                // Should we try again?
                if (e instanceof UnavailableException) {
                    retries++;
                    lastException = e;
                } else // nope, throw
                    throw e;
            }
        } while (retries < thrift.getGeneralPolicy().getMaxOpRetries());

        throw lastException;
    }

    public void release() {
        conn.release(false);
    }
}
