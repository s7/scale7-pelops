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

package org.scale7.cassandra.pelops.pool;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.scale7.cassandra.pelops.JmxMBeanManager;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

/**
* A pooled node class used by the {@link org.scale7.cassandra.pelops.pool.CommonsBackedPool}.
*/
public class PooledNode implements PooledNodeMBean {
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

        logger.info("Registering MBean '{}'...", beanName);
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

    @Override
    public void suspendIndefinetily() {
        setSuspensionState(TimeBasedSuspensionState.indefinetily());
        reportSuspension();
    }

    @Override
    public void suspendForMillis(long nodeSuspensionMillis) {
        setSuspensionState(TimeBasedSuspensionState.millisFromNow(nodeSuspensionMillis));
        reportSuspension();
    }

    @Override
    public void clearSuspensionState() {
        setSuspensionState(null);
    }

    private String getMBeanName() {
        return JMX_MBEAN_OBJ_NAME + "-" + pool.getKeyspace() + "-" + getAddress();
    }

    public static class TimeBasedSuspensionState implements CommonsBackedPool.INodeSuspensionState {
        private long suspendedUntil;

        private TimeBasedSuspensionState(long suspendedUntil) {
            this.suspendedUntil = suspendedUntil;
        }

        @Override
        public boolean isSuspended() {
            return suspendedUntil >= System.currentTimeMillis();
        }

        public static CommonsBackedPool.INodeSuspensionState millisFromNow(long millisFromNow) {
            return new TimeBasedSuspensionState(System.currentTimeMillis() + millisFromNow);
        }

        public static CommonsBackedPool.INodeSuspensionState indefinetily() {
            return new TimeBasedSuspensionState(Long.MAX_VALUE);
        }
    }
}
