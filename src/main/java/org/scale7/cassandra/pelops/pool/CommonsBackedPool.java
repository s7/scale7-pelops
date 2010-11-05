package org.scale7.cassandra.pelops.pool;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.thrift.TException;
import org.scale7.cassandra.pelops.*;
import org.scale7.cassandra.pelops.exceptions.NoConnectionsAvailableException;
import org.scale7.cassandra.pelops.exceptions.PelopsException;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

import java.net.SocketException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class CommonsBackedPool extends ThriftPoolBase implements CommonsBackedPoolMBean {
    private static final Logger logger = SystemProxy.getLoggerFromFactory(CommonsBackedPool.class);

    private static final int DEFAULT_WAIT_PERIOD = 100;

    private final Cluster cluster;

    private final Policy policy;
    private final String keyspace;
    private final OperandPolicy operandPolicy;

    private final INodeSelectionStrategy nodeSelectionStrategy;
    private final INodeSuspensionStrategy nodeSuspensionStrategy;
    private final IConnectionValidator connectionValidator;

    private final Map<String, PooledNode> nodes = new ConcurrentHashMap<String, PooledNode>();
    private GenericKeyedObjectPool pool;

    private ScheduledExecutorService executorService;
    private final Object scheduledTasksLock = new Object();

    /* running stats */
    private RunningStatistics statistics;

    /**
     * Create a new instance with reasonable defaults.
     * @param cluster the cluster this pool is pooling connections to
     * @param keyspace the keyspace this pool is for
     * @see #CommonsBackedPool(org.scale7.cassandra.pelops.Cluster, String, org.scale7.cassandra.pelops.pool.CommonsBackedPool.Policy,org.scale7.cassandra.pelops.OperandPolicy, org.scale7.cassandra.pelops.pool.CommonsBackedPool.INodeSelectionStrategy, org.scale7.cassandra.pelops.pool.CommonsBackedPool.INodeSuspensionStrategy, org.scale7.cassandra.pelops.pool.CommonsBackedPool.IConnectionValidator)
     */
    public CommonsBackedPool(Cluster cluster, String keyspace) {
        this(cluster, keyspace, new Policy(), new OperandPolicy());
    }

    /**
     * Create a new instance with reasonable defaults.
     * @param cluster the cluster this pool is pooling connections to
     * @param keyspace the keyspace this pool is for
     * @param policy the pool config
     * @param operandPolicy the operand config
     * @see #CommonsBackedPool(org.scale7.cassandra.pelops.Cluster, String, org.scale7.cassandra.pelops.pool.CommonsBackedPool.Policy,org.scale7.cassandra.pelops.OperandPolicy, org.scale7.cassandra.pelops.pool.CommonsBackedPool.INodeSelectionStrategy, org.scale7.cassandra.pelops.pool.CommonsBackedPool.INodeSuspensionStrategy, org.scale7.cassandra.pelops.pool.CommonsBackedPool.IConnectionValidator)
     */
    public CommonsBackedPool(Cluster cluster, String keyspace, Policy policy, OperandPolicy operandPolicy) {
        this(cluster, keyspace, policy, operandPolicy, null, null, null);
    }

    /**
     * Create a new instance of the pool.
     * @param cluster the cluster this pool is pooling connections to
     * @param keyspace the keyspace this pool is for
     * @param policy the pool config
     * @param operandPolicy the operand config
     * @param nodeSelectionStrategy the node selection strategy (if null then {@link org.scale7.cassandra.pelops.pool.LeastLoadedNodeSelectionStrategy} is used)
     * @param nodeSuspensionStrategy the node suspend strategy (if null then {@link org.scale7.cassandra.pelops.pool.NoOpNodeSuspensionStrategy} is used)
     * @param connectionValidator validator used to validate idle connections (if null then {@link org.scale7.cassandra.pelops.pool.DescribeVersionConnectionValidator} is used)
     */
    public CommonsBackedPool(Cluster cluster, String keyspace, Policy policy, OperandPolicy operandPolicy, INodeSelectionStrategy nodeSelectionStrategy, INodeSuspensionStrategy nodeSuspensionStrategy, IConnectionValidator connectionValidator) {
        if (cluster == null) throw new IllegalArgumentException("cluster is a required argument");
        if (keyspace == null) throw new IllegalArgumentException("keyspace is a required argument");
        this.cluster = cluster;
        this.keyspace = keyspace;
        
        this.policy = policy != null ? policy : new Policy();
        this.operandPolicy = operandPolicy != null ? operandPolicy : new OperandPolicy();

        logger.info("Initialising pool configuration policy: {}", this.policy.toString());

        this.nodeSelectionStrategy = nodeSelectionStrategy != null ? nodeSelectionStrategy : new LeastLoadedNodeSelectionStrategy();
        logger.info("Initialising pool node selection strategy: {}", this.nodeSelectionStrategy);

        this.nodeSuspensionStrategy = nodeSuspensionStrategy != null ? nodeSuspensionStrategy : new NoOpNodeSuspensionStrategy();
        logger.info("Initialising pool node suspension strategy: {}", this.nodeSuspensionStrategy);

        this.connectionValidator = connectionValidator != null ? connectionValidator : new DescribeVersionConnectionValidator();
        logger.info("Initialising pool connection validator: {}", this.connectionValidator);

        this.statistics = new RunningStatistics();

        configureBackingPool();

        Cluster.Node[] currentNodes = cluster.getNodes();
        logger.info("Pre-initialising connections for nodes: {}", Arrays.toString(currentNodes));
        for (Cluster.Node node : currentNodes) {
            addNode(node.getAddress());
        }
        statistics.nodesActive.set(this.nodes.size());

        configureScheduledTasks();

        // JMX registration
        String beanName = getMBeanName();
        if (JmxMBeanManager.getInstance().isRegistered(beanName)) {
            logger.warn("MBean '{}' is already registered, removing...", beanName);
            JmxMBeanManager.getInstance().unregisterMBean(beanName);
        }

        logger.warn("Registering MBean '{}'...", beanName);
        JmxMBeanManager.getInstance().registerMBean(this, beanName);
    }

    private void configureScheduledTasks() {
        if (policy.getTimeBetweenScheduledMaintenanceTaskRunsMillis() > 0) {
            if (Policy.MIN_TIME_BETWEEN_SCHEDULED_TASKS >= policy.getTimeBetweenScheduledMaintenanceTaskRunsMillis()) {
                logger.warn("Setting the scheduled tasks to run less than every {} milliseconds is not a good idea...", Policy.MIN_TIME_BETWEEN_SCHEDULED_TASKS);
            }

            logger.info("Configuring scheduled tasks to run every {} milliseconds", policy.getTimeBetweenScheduledMaintenanceTaskRunsMillis());
            executorService = Executors.newScheduledThreadPool(1, new ThreadFactory() {
                @Override
                public Thread newThread(Runnable runnable) {
                    Thread thread = new Thread(runnable, "pelops-pool-watcher-" + getKeyspace());
                    thread.setDaemon(false); // don't make the JVM wait for this thread to exit
                    thread.setPriority(Thread.MIN_PRIORITY + 1); // try not to disrupt other threads
                    return thread;
                }
            });

            executorService.scheduleWithFixedDelay(
                    new Runnable() {
                        @Override
                        public void run() {
                            logger.debug("Background thread running maintenance tasks");
                            try {
                                runMaintenanceTasks();
                            } catch (Exception e) {
                                logger.warn("An exception was thrown while running the maintenance tasks", e);
                            }
                        }
                    },
                    policy.getTimeBetweenScheduledMaintenanceTaskRunsMillis(),
                    policy.getTimeBetweenScheduledMaintenanceTaskRunsMillis(),
                    TimeUnit.MILLISECONDS
            );
        } else {
            logger.warn("Disabling maintenance tasks; dynamic node discovery, node suspension, idle connection " +
                    "termination and some running statistics will not be available to this pool.");
        }
    }

    @Override
    public void runMaintenanceTasks() {
        logger.debug("Attempting to acquire lock for maintenance tasks");
        synchronized (scheduledTasksLock) {
            logger.debug("Starting maintenance tasks");
            // the policy properties could have been changed by JMX, propagate them to the underlying pool
            logger.debug("Updating pool configuration properties based on policy: {}", policy);
            pool.setTestWhileIdle(policy.isTestConnectionsWhileIdle());
            pool.setMaxIdle(policy.getMaxIdlePerNode());
            pool.setMinIdle(policy.getMinIdlePerNode());
            pool.setMaxActive(policy.getMaxActivePerNode());
            pool.setMaxTotal(policy.getMaxTotal());

            // add/remove any new/dead nodes
            handleClusterRefresh();

            // check which nodes should be suspended
            logger.debug("Evaluating which nodes should be suspended");
            int nodesSuspended = 0;
            for (PooledNode node : nodes.values()) {
                logger.debug("Evaluating if node {} should be suspended", node.getAddress());
                if (nodeSuspensionStrategy.evaluate(this, node)) {
                    nodesSuspended++;
                    logger.info("Node {} was suspended from the pool, closing existing pooled connections", node.getAddress());
                    // remove any existing connections
                    pool.clear(node.getAddress());
                    node.reportSuspension();
                }
            }
            statistics.nodesActive.set(nodes.size() - nodesSuspended);
            statistics.nodesSuspended.set(nodesSuspended);

            try {
                logger.debug("Validating and possibly evicting idle connections based on configuration rules");
                pool.evict();
            } catch (Exception e) {
                // do nothing
            }
            logger.debug("Finished maintenance tasks");
        }
    }

    @Override
    public void shutdown() {
        // unregister the JMX bean
        String beanName = getMBeanName();
        logger.info("Removing MBean '{}'...", beanName);
        if (JmxMBeanManager.getInstance().isRegistered(beanName))
            JmxMBeanManager.getInstance().unregisterMBean(beanName);

        if (executorService != null) {
            logger.info("Terminating background thread...");
            executorService.shutdownNow();
            while (executorService.isTerminated()) {
                try {
                    if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                        logger.info("Still waiting for background thread to terminate...");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        try {
            logger.info("Closing pooled connections...");
            pool.close();
        } catch (Exception e) {
            logger.error("Failed to close pool", e);
        }

        // decommission the pooled nodes
        for (PooledNode pooledNode : nodes.values()) {
            logger.info("Decommissioning node '{}'", pooledNode.getAddress());
            pooledNode.decommission();
        }
    }

    @Override
    public IPooledConnection getConnection() throws NoConnectionsAvailableException {
        return getConnectionExcept(null);
    }

    @Override
    public IPooledConnection getConnectionExcept(String notNodeHint) throws NoConnectionsAvailableException {
        PooledNode node = null;
        IPooledConnection connection = null;
        long timeout = -1;

        while (connection == null) {
            if (timeout == -1) {
                // first run through calc the timeout for the next loop
                // (this makes debugging easier)
                int maxWait = getPolicy().getMaxWaitForConnection();
                timeout = maxWait > 0 ?
                        System.currentTimeMillis() + maxWait :
                        Long.MAX_VALUE;
            } else if (timeout < System.currentTimeMillis()) {
                logger.debug("Max wait time for connection exceeded");
                break;
            }

            node = nodeSelectionStrategy.select(this, nodes.keySet(), notNodeHint);
            // if the strategy was unable to choose a node (all suspended?) then sleep for a bit and loop
            if (node == null) {
                logger.debug("The node selection strategy was unable to choose a node, sleeping before trying again...");
                try {
                    Thread.sleep(DEFAULT_WAIT_PERIOD);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                continue;
            }

            try {
                logger.debug("Attempting to borrow free connection for node '{}'", node.getAddress());
                // note that if no connections are currently available for this node then the pool will sleep for
                // DEFAULT_WAIT_PERIOD milliseconds
                connection = (IPooledConnection) pool.borrowObject(node.getAddress());
            } catch (NoSuchElementException e) {
                logger.debug("No free connections available for node '{}', trying another node...", node.getAddress());
            } catch (IllegalStateException e) {
                throw new PelopsException("The pool has been shutdown", e);
            } catch (Exception e) {
                logger.warn(String.format("An exception was thrown while attempting to create a connection to '%s', " +
                        "trying another node...", node.getAddress()), e);
            }
        }

        if (node == null) {
            logger.error(
                    "Failed to get a connection within the configured wait time because there are no available nodes. " +
                            "This possibly indicates that either the suspension strategy is too aggressive or that your " +
                            "cluster is in a bad way."
            );
            throw new NoConnectionsAvailableException("Failed to get a connection within the configured max wait time.");
        }

        if (connection == null) {
            logger.error(
                    "Failed to get a connection within the maximum allowed wait time.  " +
                            "Try increasing the either the number of allowed connections or the max wait time."
            );
            throw new NoConnectionsAvailableException("Failed to get a connection within the configured max wait time.");
        }

        logger.debug("Borrowing connection '{}'", connection);
        statistics.connectionsActive.incrementAndGet();
        reportConnectionBorrowed(connection.getNode().getAddress());
        return connection;
    }

    /**
     * Returns the pooled node instance for the nodeAddress.
     *
     * @param nodeAddress the node address
     * @return the pooled node instance or null if the nodeAddress doesn't match a pooled node
     */
    public PooledNode getPooledNode(String nodeAddress) {
        return nodes.get(nodeAddress);
    }

    protected void configureBackingPool() {
        pool = new GenericKeyedObjectPool(new ConnectionFactory());
        pool.setWhenExhaustedAction(GenericKeyedObjectPool.WHEN_EXHAUSTED_BLOCK);
        pool.setMaxWait(DEFAULT_WAIT_PERIOD);
        pool.setLifo(true);
        pool.setMaxActive(policy.getMaxActivePerNode());
        pool.setMinIdle(policy.getMinIdlePerNode());
        pool.setMaxIdle(policy.getMaxIdlePerNode());
        pool.setMaxTotal(policy.getMaxTotal());
        pool.setTimeBetweenEvictionRunsMillis(-1); // we don't want to eviction thread running
        pool.setTestWhileIdle(policy.isTestConnectionsWhileIdle());
    }

    private void handleClusterRefresh() {
        cluster.refresh(getKeyspace());
        Cluster.Node[] currentNodes = cluster.getNodes();
        logger.debug("Determining which nodes need to be added and removed based on latest nodes list");
        // figure out which of the nodes are new
        for (Cluster.Node node : currentNodes) {
            if (!this.nodes.containsKey(node.getAddress())) {
                addNode(node.getAddress());
            }
        }

        // figure out which nodes need to be removed
        for (String nodeAddress : this.nodes.keySet()) {
            boolean isPresent = false;
            for (Cluster.Node node : currentNodes) {
                if (node.getAddress().equals(nodeAddress)) {
                    isPresent = true;
                }
            }

            if (!isPresent) {
                removeNode(nodeAddress);
            }
        }
    }

    private void addNode(String nodeAddress) {
        logger.info("Adding node '{}' to the pool...", nodeAddress);

        // initialise (JMX etc)
        PooledNode node = new PooledNode(this, nodeAddress);

        // add it as a candidate
        nodes.put(nodeAddress, node);

        // prepare min idle connetions etc...
        // NOTE: there's a potential for the node to be selected as a candidate before it's been prepared
        //       but preparing before adding means the stats don't get updated
        pool.preparePool(nodeAddress, true);
    }

    private void removeNode(String nodeAddress) {
        logger.info("Removing node '{}' from the pool", nodeAddress);

        // remove from the the nodes list so it's no longer considered a candidate
        PooledNode node = nodes.remove(nodeAddress);

        // shutdown all the connections and clear it from the backing pool
        pool.clear(nodeAddress);

        // decommission (JMX etc)
        if (node != null) {
            node.decommission();
        }
    }

    protected void releaseConnection(PooledConnection connection) {
        try {
            statistics.connectionsActive.decrementAndGet();
            reportConnectionReleased(connection.getNode().getAddress());

            if (connection.isCorrupt() || !connection.isOpen()) {
                logger.debug("Returned connection '{}' has been closed or is marked as corrupt", connection);
                reportConnectionCorrupted(connection.getNode().getAddress());
                pool.invalidateObject(connection.getNode().getAddress(), connection);
            } else {
                logger.debug("Returning connection '{}'", connection);
                pool.returnObject(connection.getNode().getAddress(), connection);
            }
        } catch (Exception e) {
            // do nothing
        }
    }

    protected void reportConnectionCreated(String nodeAddress) {
        statistics.connectionsCreated.incrementAndGet();

        PooledNode node = getPooledNode(nodeAddress);

        if (node != null)
            node.reportConnectionCreated();
    }

    protected void reportConnectionDestroyed(String nodeAddress) {
        statistics.connectionsDestroyed.incrementAndGet();

        PooledNode node = getPooledNode(nodeAddress);

        if (node != null)
            node.reportConnectionDestroyed();
    }

    protected void reportConnectionCorrupted(String nodeAddress) {
        statistics.connectionsCorrupted.incrementAndGet();

        PooledNode pooledNode = getPooledNode(nodeAddress);

        if (pooledNode != null)  // it's possible that the pooled node has been removed
            pooledNode.reportConnectionCorrupted();
    }

    protected void reportConnectionBorrowed(String nodeAddress) {
        statistics.connectionsBorrowedTotal.incrementAndGet();

        PooledNode pooledNode = getPooledNode(nodeAddress);

        if (pooledNode != null)  // it's possible that the pooled node has been removed
            pooledNode.reportConnectionBorrowed();
    }

    protected void reportConnectionReleased(String nodeAddress) {
        statistics.connectionsReleasedTotal.incrementAndGet();

        PooledNode pooledNode = getPooledNode(nodeAddress);

        if (pooledNode != null)  // it's possible that the pooled node has been removed
            pooledNode.reportConnectionReleased();
    }

    @Override
    public OperandPolicy getOperandPolicy() {
        return operandPolicy;
    }

    @Override
    public String getKeyspace() {
        return keyspace;
    }

    public Policy getPolicy() {
        return policy;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public INodeSelectionStrategy getNodeSelectionStrategy() {
        return nodeSelectionStrategy;
    }

    public INodeSuspensionStrategy getNodeSuspensionStrategy() {
        return nodeSuspensionStrategy;
    }

    public IConnectionValidator getConnectionValidator() {
        return connectionValidator;
    }

    public RunningStatistics getStatistics() {
        return statistics;
    }

    @Override
    public int getConnectionsCreated() {
        return getStatistics().getConnectionsCreated();
    }

    @Override
    public int getConnectionsDestroyed() {
        return getStatistics().getConnectionsCreated();
    }

    @Override
    public int getConnectionsCorrupted() {
        return getStatistics().getConnectionsCorrupted();
    }

    @Override
    public int getConnectionsActive() {
        return getStatistics().getConnectionsActive();
    }

    @Override
    public int getNodesActive() {
        return getStatistics().getNodesActive();
    }

    @Override
    public int getNodesSuspended() {
        return getStatistics().getNodesSuspended();
    }

    @Override
    public int getConnectionsBorrowedTotal() {
        return getStatistics().getConnectionsBorrowedTotal();
    }

    @Override
    public int getConnectionsReleasedTotal() {
        return getStatistics().getConnectionsReleasedTotal();
    }

    @Override
    public int getMaxActivePerNode() {
        return getPolicy().getMaxActivePerNode();
    }

    @Override
    public void setMaxActivePerNode(int maxActivePerNode) {
        getPolicy().setMaxActivePerNode(maxActivePerNode);
    }

    @Override
    public int getMaxIdlePerNode() {
        return getPolicy().getMaxIdlePerNode();
    }

    @Override
    public void setMaxIdlePerNode(int maxIdlePerNode) {
        getPolicy().setMaxIdlePerNode(maxIdlePerNode);
    }

    @Override
    public int getMaxTotal() {
        return getPolicy().getMaxTotal();
    }

    @Override
    public void setMaxTotal(int maxTotal) {
        getPolicy().setMaxTotal(maxTotal);
    }

    @Override
    public int getMinIdlePerNode() {
        return getPolicy().getMinIdlePerNode();
    }

    @Override
    public void setMinIdlePerNode(int minIdlePerNode) {
        getPolicy().setMinIdlePerNode(minIdlePerNode);
    }

    @Override
    public int getMaxWaitForConnection() {
        return getPolicy().getMaxWaitForConnection();
    }

    @Override
    public void setMaxWaitForConnection(int maxWaitForConnection) {
        getPolicy().setMaxWaitForConnection(maxWaitForConnection);
    }

    @Override
    public boolean isTestConnectionsWhileIdle() {
        return getPolicy().isTestConnectionsWhileIdle();
    }

    @Override
    public void setTestConnectionsWhileIdle(boolean testConnectionsWhileIdle) {
        getPolicy().setTestConnectionsWhileIdle(testConnectionsWhileIdle);
    }

    private String getMBeanName() {
        return JMX_MBEAN_OBJ_NAME + "-" + keyspace;
    }

    KeyedObjectPool getUnderlyingPool() {
        return pool;
    }

    public static class Policy {
        private static final int DEFAULT_TIME_BETWEEN_SCHEDULED_TASKS = 1000 * 60;
        private static final int MIN_TIME_BETWEEN_SCHEDULED_TASKS = 1000 * 10;

        private AtomicInteger maxActivePerNode = new AtomicInteger(20);
        private AtomicInteger maxTotal = new AtomicInteger(-1);
        private AtomicInteger maxIdlePerNode = new AtomicInteger(10);
        private AtomicInteger minIdlePerNode = new AtomicInteger(10);
        private AtomicInteger maxWaitForConnection = new AtomicInteger(1000);
        private int timeBetweenScheduledMaintenanceTaskRunsMillis = DEFAULT_TIME_BETWEEN_SCHEDULED_TASKS;
        private AtomicBoolean testConnectionsWhileIdle = new AtomicBoolean(true);

        public Policy() {
        }

        /**
         * @see #setMaxActivePerNode(int)
         */
        public int getMaxActivePerNode() {
            return maxActivePerNode.get();
        }

        /**
         * Sets the cap on the number of object instances managed by the pool per node.
         *
         * @param maxActivePerNode The cap on the number of object instances per node. Use a negative value for no limit.
         */
        public void setMaxActivePerNode(int maxActivePerNode) {
            this.maxActivePerNode.set(maxActivePerNode);
        }

        /**
         * @see #setMaxActivePerNode(int)
         */
        public int getMaxIdlePerNode() {
            return maxIdlePerNode.get();
        }

        /**
         * Sets the cap on the number of "idle" instances in the pool. If maxIdle is set too low on heavily loaded
         * systems it is possible you will see objects being destroyed and almost immediately new objects being created.
         * This is a result of the active threads momentarily returning objects faster than they are requesting them
         * them, causing the number of idle objects to rise above maxIdle. The best value for maxIdle for heavily
         * loaded system will vary but the default is a good starting point.
         *
         * @param maxIdlePerNode
         */
        public void setMaxIdlePerNode(int maxIdlePerNode) {
            this.maxIdlePerNode.set(maxIdlePerNode);
        }

        /**
         * @see #setMaxTotal(int)
         */
        public int getMaxTotal() {
            return maxTotal.get();
        }

        /**
         * Sets the cap on the total number of instances from all nodes combined. When maxTotal is set to a positive
         * value and {@link CommonsBackedPool#getConnection()} is invoked when at the limit with no idle instances
         * available, an attempt is made to create room by clearing the oldest 15% of the elements from the keyed pools.
         *
         * @param maxTotal The cap on the number of object instances per node. Use a negative value for no limit.
         */
        public void setMaxTotal(int maxTotal) {
            this.maxTotal.set(maxTotal);
        }

        /**
         * @see #setMinIdlePerNode(int)
         */
        public int getMinIdlePerNode() {
            return minIdlePerNode.get();
        }

        /**
         * Sets the minimum number of idle objects to maintain in each of the nodes.
         *
         * @param minIdlePerNode The minimum size of the each nodes pool
         */
        public void setMinIdlePerNode(int minIdlePerNode) {
            this.minIdlePerNode.set(minIdlePerNode);
        }

        /**
         * @see #setMaxWaitForConnection(int)
         */
        public int getMaxWaitForConnection() {
            return maxWaitForConnection.get();
        }

        /**
         * Sets the maximum amount of time (in milliseconds) the {@link CommonsBackedPool#getConnection()} method should
         * wait before throwing an exception when the pool is exhausted.  When less than or equal to 0, the
         * {@link CommonsBackedPool#getConnection()} method may block indefinitely.
         *
         * @param maxWaitForConnection the maximum number of milliseconds {@link CommonsBackedPool#getConnection()}
         *                             will block or negative for indefinitely.
         */
        public void setMaxWaitForConnection(int maxWaitForConnection) {
            this.maxWaitForConnection.set(maxWaitForConnection);
        }

        /**
         * @see #setTimeBetweenScheduledMaintenanceTaskRunsMillis(int)
         */
        public int getTimeBetweenScheduledMaintenanceTaskRunsMillis() {
            return timeBetweenScheduledMaintenanceTaskRunsMillis;
        }

        /**
         * Sets the number of milliseconds to sleep between runs of the idle object tasks thread. When non-positive,
         * no idle object evictor thread will be run.
         *
         * @param timeBetweenScheduledMaintenanceTaskRunsMillis
         *         milliseconds to sleep between evictor runs.
         */
        public void setTimeBetweenScheduledMaintenanceTaskRunsMillis(int timeBetweenScheduledMaintenanceTaskRunsMillis) {
            this.timeBetweenScheduledMaintenanceTaskRunsMillis = timeBetweenScheduledMaintenanceTaskRunsMillis;
        }

        /**
         * @see #setTestConnectionsWhileIdle(boolean) 
         */
        public boolean isTestConnectionsWhileIdle() {
            return testConnectionsWhileIdle.get();
        }

        /**
         * When true, connections will be validated by scheduled tasks thread (if enabled). If an connection fails to
         * validate, it will be dropped from the pool.
         * @param testConnectionsWhileIdle true if enabled, otherwise false
         */
        public void setTestConnectionsWhileIdle(boolean testConnectionsWhileIdle) {
            this.testConnectionsWhileIdle.set(testConnectionsWhileIdle);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("Config");
            sb.append("{maxActivePerNode=").append(maxActivePerNode);
            sb.append(", maxTotal=").append(maxTotal);
            sb.append(", maxIdlePerNode=").append(maxIdlePerNode);
            sb.append(", minIdlePerNode=").append(minIdlePerNode);
            sb.append(", maxWaitForConnection=").append(maxWaitForConnection);
            sb.append(", testConnectionsWhileIdle=").append(testConnectionsWhileIdle);
            sb.append(", timeBetweenScheduledMaintenanceTaskRunsMillis=").append(timeBetweenScheduledMaintenanceTaskRunsMillis);
            sb.append('}');
            return sb.toString();
        }
    }

    public class PooledConnection extends Connection implements IPooledConnection {
        private boolean corrupt = false;

        public PooledConnection(Cluster.Node node, String keyspace) throws SocketException, TException, InvalidRequestException {
            super(node, keyspace);
        }

        @Override
        public void release() {
            releaseConnection(this);
        }

        @Override
        public void corrupted() {
            corrupt = true;
        }

        public boolean isCorrupt() {
            return corrupt;
        }

        @Override
        public String toString() {
            return String.format("Connection[%s][%s:%s][%s]", getKeyspace(), getNode().getAddress(), cluster.getConnectionConfig().getThriftPort(), super.hashCode());
        }
    }

    private class ConnectionFactory extends BaseKeyedPoolableObjectFactory {
        @Override
        public Object makeObject(Object key) throws Exception {
            String nodeAddress = (String) key;
            PooledConnection connection = new PooledConnection(
                    new Cluster.Node(nodeAddress, cluster.getConnectionConfig()), getKeyspace()
            );
            logger.debug("Made new connection '{}'", connection);
            connection.open();

            reportConnectionCreated(nodeAddress);

            return connection;
        }

        @Override
        public void destroyObject(Object key, Object obj) throws Exception {
            String nodeAddress = (String) key;
            PooledConnection connection = (PooledConnection) obj;

            logger.debug("Destroying connection '{}'", connection);

            connection.close();

            reportConnectionDestroyed(nodeAddress);
        }

        @Override
        public boolean validateObject(Object key, Object obj) {
            String nodeAddress = (String) key;
            PooledConnection connection = (PooledConnection) obj;

            logger.debug("Validating connection '{}'", connection);

            return connectionValidator.validate(connection);
        }

        @Override
        public void activateObject(Object key, Object obj) throws Exception {
        }

        @Override
        public void passivateObject(Object key, Object obj) throws Exception {
        }
    }

    public static class RunningStatistics {
        private AtomicInteger nodesActive;
        private AtomicInteger nodesSuspended;
        private AtomicInteger connectionsCreated;
        private AtomicInteger connectionsDestroyed;
        private AtomicInteger connectionsCorrupted;
        private AtomicInteger connectionsActive;
        private AtomicInteger connectionsBorrowedTotal;
        private AtomicInteger connectionsReleasedTotal;

        public RunningStatistics() {
            nodesActive = new AtomicInteger();
            nodesSuspended = new AtomicInteger();
            connectionsCreated = new AtomicInteger();
            connectionsDestroyed = new AtomicInteger();
            connectionsCorrupted = new AtomicInteger();
            connectionsActive = new AtomicInteger();
            connectionsBorrowedTotal = new AtomicInteger();
            connectionsReleasedTotal = new AtomicInteger();
        }

        public int getConnectionsCreated() {
            return connectionsCreated.get();
        }

        public int getConnectionsDestroyed() {
            return connectionsDestroyed.get();
        }

        public int getConnectionsCorrupted() {
            return connectionsCorrupted.get();
        }

        public int getConnectionsActive() {
            return connectionsActive.get();
        }

        public int getNodesActive() {
            return nodesActive.get();
        }

        public int getNodesSuspended() {
            return nodesSuspended.get();
        }

        public int getConnectionsBorrowedTotal() {
            return connectionsBorrowedTotal.get();
        }

        public int getConnectionsReleasedTotal() {
            return connectionsReleasedTotal.get();
        }
    }

    /**
     * Interface used to define how nodes should be selected.
     */
    public static interface INodeSelectionStrategy {
        /**
         * Called when a node need to be selected.
         *
         * @param pool          the pool (just in case you need it)
         * @param nodeAddresses the node addresses to select from
         * @param notNodeHint   a hint of the node address that the selection strategy should avoid (possible null)
         * @return the selected node (null if none are available)
         */
        PooledNode select(CommonsBackedPool pool, Set<String> nodeAddresses, String notNodeHint);
    }

    /**
     * Interface used to define how nodes should be suspended for behaving badly.  For example, if a
     * node is reporting lots of corrupt connections then maybe it should be avoided for a while.
     * <p/>
     * <p>Implementations should indicate if a node is suspended by ensuring that
     * {@link org.scale7.cassandra.pelops.pool.CommonsBackedPool.INodeSuspensionState#isSuspended()} returns true
     * until the node should no longer be considered suspended.
     * <p/>
     * <p>Any state required to determine if a node should be suspended should be stored in the nodes
     * {@link PooledNode#getSuspensionState()}.  Note that the
     * suspension state may be null if the node has not been evaluated before.
     * <p>Also note that the {@link #evaluate(CommonsBackedPool, PooledNode)}
     * will be called by the scheduled tasks thread even when the node is currently suspended.
     */
    public static interface INodeSuspensionStrategy {
        /**
         * Called for each node in the pool by the pools background thread.
         *
         * @param pool the pool (just in case you need it)
         * @param node the node to evaluate
         * @return true if the node was suspending, otherwise false
         */
        boolean evaluate(CommonsBackedPool pool, PooledNode node);
    }

    /**
     * Interface used to define a pooled nodes suspension status.
     *
     * @see INodeSuspensionStrategy
     */
    public static interface INodeSuspensionState {
        /**
         * Used to indicate if a node is suspended.
         *
         * <p><b>Note</b>: Implementations needs to ensure that invokations to this method are thread safe.
         *
         * @return true if the node is suspended, otherwise false (this method should return true until the node is no
         *         longer considered suspended)
         */
        boolean isSuspended();
    }

    /**
     * Interface used to define how a connection is validated while idle.
     *
     * @see INodeSuspensionStrategy
     */
    public static interface IConnectionValidator {
        /**
         * Used to indicate if a connection is valid.
         *
         * @param connection the connection
         * @return true if the connection is valid, otherwise false
         */
        boolean validate(PooledConnection connection);
    }
}
