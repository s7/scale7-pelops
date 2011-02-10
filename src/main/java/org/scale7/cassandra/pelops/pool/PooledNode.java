package org.scale7.cassandra.pelops.pool;

import org.scale7.cassandra.pelops.JmxMBeanManager;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
* A pooled node class used by the {@link org.scale7.cassandra.pelops.pool.CommonsBackedPool}.
*/
class PooledNode implements PooledNodeMBean {
    private static final Logger logger = SystemProxy.getLoggerFromFactory(CommonsBackedPool.class);
    private CommonsBackedPool pool;
    private String address;

    private CommonsBackedPool.INodeSuspensionState suspensionState;
    private final ReentrantReadWriteLock suspensionStateLock = new ReentrantReadWriteLock();
    private final Lock suspensionStateReadLock = suspensionStateLock.readLock();
    private final Lock suspensionStateWriteLock = suspensionStateLock.writeLock();

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
        if (JmxMBeanManager.getInstance().isRegistered(beanName)) {
            logger.warn("MBean '{}' is already registered, removing...", beanName);
            JmxMBeanManager.getInstance().unregisterMBean(beanName);
        }

        logger.warn("Registering MBean '{}'...", beanName);
        JmxMBeanManager.getInstance().registerMBean(this, beanName);
    }

    public void decommission() {
        String beanName = getMBeanName();
        logger.info("Removing MBean '{}'...", beanName);
        if (JmxMBeanManager.getInstance().isRegistered(beanName))
            JmxMBeanManager.getInstance().unregisterMBean(beanName);
    }

    @Override
    public String getAddress() {
        return address;
    }

    public CommonsBackedPool.INodeSuspensionState getSuspensionState() {
        try {
            suspensionStateReadLock.lock();
            return suspensionState;
        } finally {
            suspensionStateReadLock.unlock();
        }
    }

    public void setSuspensionState(CommonsBackedPool.INodeSuspensionState suspensionState) {
        try {
            suspensionStateWriteLock.lock();
            this.suspensionState = suspensionState;
        } finally {
            suspensionStateWriteLock.unlock();
        }
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
        try {
            suspensionStateReadLock.lock();
            CommonsBackedPool.INodeSuspensionState state = getSuspensionState();
            return state != null && state.isSuspended();
        } finally {
            suspensionStateReadLock.unlock();
        }
    }

    private String getMBeanName() {
        return JMX_MBEAN_OBJ_NAME + "-" + pool.getKeyspace();
    }
}
