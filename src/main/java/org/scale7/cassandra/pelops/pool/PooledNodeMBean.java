package org.scale7.cassandra.pelops.pool;

public interface PooledNodeMBean {
    String JMX_MBEAN_OBJ_NAME = "com.scale7.cassandra.pelops.pool:type=PooledNode";

    String getAddress();

    int getSuspensions();

    int getNumActive();

    int getNumIdle();

    int getConnectionsCorrupted();

    int getConnectionsCreated();

    int getConnectionsDestroyed();

    int getConnectionsBorrowedTotal();

    int getConnectionsReleasedTotal();

    boolean isSuspended();

    void suspendIndefinetily();

    void suspendForMillis(long nodeSuspensionMillis);

    void clearSuspensionState();
}
