package org.scale7.cassandra.pelops.pool;

public interface CommonsBackedPoolMBean {
    String JMX_MBEAN_OBJ_NAME = "com.scale7.cassandra.pelops.pool:type=CommonsBackedPool";

    /*
        RUNNING STATS
     */

    int getConnectionsCreated();

    int getConnectionsDestroyed();

    int getConnectionsCorrupted();

    int getConnectionsActive();

    int getNodesActive();

    int getNodesSuspended();

    int getConnectionsBorrowedTotal();

    int getConnectionsReleasedTotal();

    /*
        CONFIGURATION
     */

    public int getMaxActivePerNode();

    public void setMaxActivePerNode(int maxActivePerNode);

    public int getMaxIdlePerNode();

    public void setMaxIdlePerNode(int maxIdlePerNode);

    public int getMaxTotal();

    public void setMaxTotal(int maxTotal);

    public int getMinIdlePerNode();

    public void setMinIdlePerNode(int minIdlePerNode);

    public int getMaxWaitForConnection();

    public void setMaxWaitForConnection(int maxWaitForConnection);

    public boolean isTestConnectionsWhileIdle();

    public void setTestConnectionsWhileIdle(boolean testConnectionsWhileIdle);

    public int getNodeDownSuspensionMillis();

    public void setNodeDownSuspensionMillis(int nodeDownSuspensionMillis);

    /*
        OPERATIONS
     */

    void runMaintenanceTasks();
}
