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
