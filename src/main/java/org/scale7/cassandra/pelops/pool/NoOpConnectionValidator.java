package org.scale7.cassandra.pelops.pool;

/**
 * A connection validator that doesn't validate the connection.
 */
public class NoOpConnectionValidator implements CommonsBackedPool.IConnectionValidator {
    /**
     * Returns true.
     * @param connection the connection
     * @return true
     */
    @Override
    public boolean validate(CommonsBackedPool.PooledConnection connection) {
        return true;
    }
}
