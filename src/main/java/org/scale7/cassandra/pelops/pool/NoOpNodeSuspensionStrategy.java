package org.scale7.cassandra.pelops.pool;

/**
 * An implementation of the {@link CommonsBackedPool.INodeSuspensionStrategy} that does nothing.
 */
public class NoOpNodeSuspensionStrategy implements CommonsBackedPool.INodeSuspensionStrategy {
    @Override
    public boolean evaluate(CommonsBackedPool pool, PooledNode node) {
        return false;
    }
}
