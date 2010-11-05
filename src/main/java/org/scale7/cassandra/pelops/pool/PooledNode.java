package org.scale7.cassandra.pelops.pool;

import org.scale7.cassandra.pelops.JmxMBeanManager;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

/**
* A pooled node class used by the {@link org.scale7.cassandra.pelops.pool.CommonsBackedPool}.
*/
class PooledNode implements PooledNodeMBean {
    private static final Logger logger = SystemProxy.getLoggerFromFactory(CommonsBackedPool.class);
    private CommonsBackedPool pool;
    private String address;
    private CommonsBackedPool.INodeSuspensionState suspensionState;
    private AtomicInteger suspensions;
    private AtomicInteger connectionsCorrupted;
    private AtomicInteger connectionsCreated;
    private AtomicInteger connectionsDestroyed;
    private AtomicInteger connectionsBorrowedTotal;
    private AtomicInteger connectionsReleasedTotal;

    PooledNode(CommonsBackedPool pool, String address) {
        this.pool = pool;
        this.address = address;
        suspensions = new AtomicInteger();
        connectionsCorrupted = new AtomicInteger();
        connectionsCreated = new AtomicInteger();
        connectionsDestroyed = new AtomicInteger();
        connectionsBorrowedTotal = new AtomicInteger();
        connectionsReleasedTotal = new AtomicInteger();

        String beanName = getMBeanName();
        logger.warn("Registering MBean '{}'...", beanName);
        JmxMBeanManager.getInstance().registerMBean(this, beanName);
    }

    public void decommission() {
        String beanName = getMBeanName();
        logger.info("Removing MBean '{}'...", beanName);
        JmxMBeanManager.getInstance().unregisterMBean(beanName);
    }

    @Override
    public String getAddress() {
        return address;
    }

    public CommonsBackedPool.INodeSuspensionState getSuspensionState() {
        return suspensionState;
    }

    public void setSuspensionState(CommonsBackedPool.INodeSuspensionState suspensionState) {
        this.suspensionState = suspensionState;
    }

    void reportSuspension() {
        suspensions.incrementAndGet();
    }

    @Override
    public int getSuspensions() {
        return suspensions.get();
    }

    @Override
    public int getNumActive() {
        return pool.getUnderlyingPool().getNumActive(address);
    }

    @Override
    public int getNumIdle() {
        return pool.getUnderlyingPool().getNumIdle(address);
    }

    void reportConnectionCorrupted() {
        connectionsCorrupted.incrementAndGet();
    }

    @Override
    public int getConnectionsCorrupted() {
        return connectionsCorrupted.get();
    }

    void reportConnectionCreated() {
        connectionsCreated.incrementAndGet();
    }

    @Override
    public int getConnectionsCreated() {
        return connectionsCreated.get();
    }

    void reportConnectionDestroyed() {
        connectionsDestroyed.incrementAndGet();
    }

    @Override
    public int getConnectionsDestroyed() {
        return connectionsDestroyed.get();
    }

    void reportConnectionBorrowed() {
        connectionsBorrowedTotal.incrementAndGet();
    }

    @Override
    public int getConnectionsBorrowedTotal() {
        return connectionsBorrowedTotal.get();
    }

    void reportConnectionReleased() {
        connectionsReleasedTotal.incrementAndGet();
    }

    @Override
    public int getConnectionsReleasedTotal() {
        return connectionsReleasedTotal.get();
    }

    @Override
    public boolean isSuspended() {
        return getSuspensionState() != null && getSuspensionState().isSuspended();
    }

    private String getMBeanName() {
        return JMX_MBEAN_OBJ_NAME + "-" + pool.getKeyspace();
    }
}
