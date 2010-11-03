package org.scale7.cassandra.pelops.pool;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.thrift.TException;
import org.scale7.cassandra.pelops.*;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

import java.net.SocketException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CommonsBackedPool extends ThriftPoolBase {
    private static final Logger logger = SystemProxy.getLoggerFromFactory(CommonsBackedPool.class);

    private static final int POOL_MAX_WAIT = 100;

    private final Cluster cluster;

    private final Map<String, PooledNode> nodes = new ConcurrentHashMap<String, PooledNode>();
    private final Config config;
    private final INodeSelectionPolicy nodeSelectionPolicy;

    private final String keyspace;

    private final OperandPolicy operandPolicy;

    private GenericKeyedObjectPool pool;

    private ScheduledExecutorService executorService;

    /* running stats */
    private AtomicInteger connectionsCreated;
    private AtomicInteger connectionsDestroyed;
    private AtomicInteger connectionsCorrupted;
    private AtomicInteger connectionsActive;

    public CommonsBackedPool(Cluster cluster, Config config, INodeSelectionPolicy nodeSelectionPolicy, OperandPolicy operandPolicy, String keyspace) {
        this.cluster = cluster;
        this.config = config;
        this.nodeSelectionPolicy = nodeSelectionPolicy;
        this.operandPolicy = operandPolicy;
        this.keyspace = keyspace;

        connectionsCreated = new AtomicInteger();
        connectionsDestroyed = new AtomicInteger();
        connectionsCorrupted = new AtomicInteger();
        connectionsActive = new AtomicInteger();

        logger.info("Initialising pool with: {}", config.toString());

        configurePool();

        Cluster.Node[] currentNodes = cluster.getNodes();
        logger.info("Pre-initialising connections for nodes: {}", Arrays.toString(currentNodes));
        for (Cluster.Node node : currentNodes) {
            addNode(node.getAddress());
        }

        configureScheduledTasks();
    }

    protected void configureScheduledTasks() {
        if (config.getTimeBetweenScheduledTaskRunsMillis() > 0) {
            logger.debug("Configuring scheduled tasks to run every {} milliseconds", config.getTimeBetweenScheduledTaskRunsMillis());
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
                            logger.debug("Running scheduled tasks");
                            scheduledTasks();
                        }
                    },
                    config.getTimeBetweenScheduledTaskRunsMillis(),
                    config.getTimeBetweenScheduledTaskRunsMillis(),
                    TimeUnit.MILLISECONDS
            );
        } else {
            logger.debug("Disabling scheduled tasks");
        }
    }

    protected void configurePool() {
        pool = new GenericKeyedObjectPool(new ConnectionFactory());
        pool.setWhenExhaustedAction(GenericKeyedObjectPool.WHEN_EXHAUSTED_BLOCK);
        pool.setMaxWait(POOL_MAX_WAIT);
        pool.setLifo(true);
        pool.setMaxActive(config.getMaxActivePerNode());
        pool.setMinIdle(config.getMinIdlePerNode());
        pool.setMaxIdle(config.getMaxIdlePerNode());
        pool.setMaxTotal(config.getMaxTotal());
        pool.setTimeBetweenEvictionRunsMillis(-1); // we don't want to eviction thread running
    }

    protected void scheduledTasks() {
        handleClusterRefresh();

        try {
            logger.debug("Evicting idle nodes based on configuration rules");
            pool.evict();
        } catch (Exception e) {
            // do nothing
        }
    }

    protected void handleClusterRefresh() {
        cluster.refresh();
        Cluster.Node[] currentNodes = cluster.getNodes();
        // to void the cluster refresh and scheduled tasks colliding 
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

    protected void addNode(String nodeAddress) {
        logger.info("Preparing connections for node '{}'", nodeAddress);

        // prepare min idle connetions etc...
        pool.preparePool(nodeAddress, true);

        // initialise (JMX etc)
        PooledNode node = new PooledNode(nodeAddress);

        // add it as a candidate
        nodes.put(nodeAddress, node);
    }

    protected void removeNode(String nodeAddress) {
        logger.info("Removing connections for node '{}'", nodeAddress);

        // remove from the the nodes list so it's no longer considered a candidate
        PooledNode node = nodes.remove(nodeAddress);

        // shutdown all the connections and clear it from the backing pool
        pool.clear(nodeAddress);

        // decommission (JMX etc)
        if (node != null) {
            node.decommission();
        }
    }

    @Override
    public IPooledConnection getConnection() throws Exception {
        IPooledConnection connection = null;
        long timeout = -1;

        while (connection == null) {
            if (timeout == -1) {
                // first run through calc the timeout for the next loop
                // (this makes debugging easier)
                timeout = getConfig().getMaxWaitForConnection() > 0 ?
                System.currentTimeMillis() + getConfig().getMaxWaitForConnection() :
                Long.MAX_VALUE;
            } else if (timeout < System.currentTimeMillis()) {
                logger.debug("Max time for connection exceeded");
                break;
            }

            PooledNode node = nodeSelectionPolicy.select(this, nodes);

            try {
                logger.debug("Attempting to borrow free connection for node '{}'", node.getAddress());
                connection = (IPooledConnection) pool.borrowObject(node.getAddress());
            } catch (NoSuchElementException e) {
                logger.debug("No free connections available for node '{}', trying another node...", node.getAddress());
            }
        }

        if (connection == null) {
            logger.warn(
                    "Failed to get a connection within the configured wait time.  " +
                            "Try increasing the either the number of allowed connections (currently {}) or the max wait " +
                            "time (currently {}).", getConfig().getMaxActivePerNode(), getConfig().getMaxWaitForConnection()
            );
            throw new TimeoutException("Failed to get a connection within the configured wait time.");
        }

        logger.debug("Borrowing connection '{}'", connection);
        connectionsActive.incrementAndGet();
        return connection;
    }

    @Override
    public IPooledConnection getConnectionExcept(String notNode) throws Exception {
        return getConnection();
    }

    protected void releaseConnection(PooledConnection connection) {
        logger.debug("Returning connection '{}'", connection);
        try {
            pool.returnObject(connection.getNode().getAddress(), connection);
            connectionsActive.decrementAndGet();
        } catch (Exception e) {
            // do nothing
        }
    }

    @Override
    public void shutdown() {
        if (executorService != null) {
            logger.debug("Terminating background thread...");
            executorService.shutdownNow();
        }

        try {
            logger.debug("Closing pooled connections...");
            pool.close();
        } catch (Exception e) {
            logger.error("Failed to close pool", e);
        }
    }

    @Override
    public OperandPolicy getOperandPolicy() {
        return operandPolicy;
    }

    @Override
    public String getKeyspace() {
        return keyspace;
    }

    public Config getConfig() {
        return config;
    }

    protected PooledNode getPooledNode(String nodeAddress) {
        return nodes.get(nodeAddress);
    }

    protected void reportConnectionCreated(String nodeAddress) {
        connectionsCreated.incrementAndGet();

        PooledNode node = getPooledNode(nodeAddress);

        if (node != null)
            node.reportConnectionCreated();
    }

    protected void reportConnectionDestroyed(String nodeAddress) {
        connectionsDestroyed.incrementAndGet();

        PooledNode node = getPooledNode(nodeAddress);

        if (node != null)
            node.reportConnectionDestroyed();
    }

    protected void reportConnectionCorrupted(String nodeAddress) {
        connectionsCorrupted.incrementAndGet();

        PooledNode pooledNode = getPooledNode(nodeAddress);

        if (pooledNode != null)  // it's possible that the pooled node has been removed
            pooledNode.reportConnectionCorrupted();
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

    public static class Config {
        private int maxActivePerNode = 20;
        private int maxTotal = -1;
        private int maxIdlePerNode = 10;
        private int minIdlePerNode = 10;
        private int maxWaitForConnection = 1000;
        private int timeBetweenScheduledTaskRunsMillis = 1000 * 60;

        public Config() {
        }

        /**
         * @see #setMaxActivePerNode(int)
         */
        public int getMaxActivePerNode() {
            return maxActivePerNode;
        }

        /**
         * Sets the cap on the number of object instances managed by the pool per node.
         * @param maxActivePerNode The cap on the number of object instances per node. Use a negative value for no limit.
         */
        public void setMaxActivePerNode(int maxActivePerNode) {
            this.maxActivePerNode = maxActivePerNode;
        }

        public int getMaxIdlePerNode() {
            return maxIdlePerNode;
        }

        /**
         * Sets the cap on the number of "idle" instances in the pool. If maxIdle is set too low on heavily loaded
         * systems it is possible you will see objects being destroyed and almost immediately new objects being created.
         * This is a result of the active threads momentarily returning objects faster than they are requesting them
         * them, causing the number of idle objects to rise above maxIdle. The best value for maxIdle for heavily
         * loaded system will vary but the default is a good starting point.
         * @param maxIdlePerNode
         */
        public void setMaxIdlePerNode(int maxIdlePerNode) {
            this.maxIdlePerNode = maxIdlePerNode;
        }

        /**
         * @see #setMaxTotal(int)
         */
        public int getMaxTotal() {
            return maxTotal;
        }

        /**
         * Sets the cap on the total number of instances from all nodes combined. When maxTotal is set to a positive
         * value and {@link CommonsBackedPool#getConnection()} is invoked when at the limit with no idle instances
         * available, an attempt is made to create room by clearing the oldest 15% of the elements from the keyed pools.
         * @param maxTotal The cap on the number of object instances per node. Use a negative value for no limit.
         */
        public void setMaxTotal(int maxTotal) {
            this.maxTotal = maxTotal;
        }

        /**
         * @see #setMinIdlePerNode(int)
         */
        public int getMinIdlePerNode() {
            return minIdlePerNode;
        }

        /**
         * Sets the minimum number of idle objects to maintain in each of the nodes.
         * @param minIdlePerNode The minimum size of the each nodes pool
         */
        public void setMinIdlePerNode(int minIdlePerNode) {
            this.minIdlePerNode = minIdlePerNode;
        }

        /**
         * @see #setMaxWaitForConnection(int)
         */
        public int getMaxWaitForConnection() {
            return maxWaitForConnection;
        }

        /**
         * Sets the maximum amount of time (in milliseconds) the {@link CommonsBackedPool#getConnection()} method should
         * wait before throwing an exception when the pool is exhausted.  When less than or equal to 0, the
         * {@link CommonsBackedPool#getConnection()} method may block indefinitely.
         * @param maxWaitForConnection the maximum number of milliseconds {@link CommonsBackedPool#getConnection()}
         *  will block or negative for indefinitely.
         */
        public void setMaxWaitForConnection(int maxWaitForConnection) {
            this.maxWaitForConnection = maxWaitForConnection;
        }

        /**
         * @see #setTimeBetweenScheduledTaskRunsMillis(int)
         */
        public int getTimeBetweenScheduledTaskRunsMillis() {
            return timeBetweenScheduledTaskRunsMillis;
        }

        /**
         * Sets the number of milliseconds to sleep between runs of the idle object tasks thread. When non-positive,
         * no idle object evictor thread will be run.
         * @param timeBetweenScheduledTaskRunsMillis milliseconds to sleep between evictor runs.
         */
        public void setTimeBetweenScheduledTaskRunsMillis(int timeBetweenScheduledTaskRunsMillis) {
            this.timeBetweenScheduledTaskRunsMillis = timeBetweenScheduledTaskRunsMillis;
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
            sb.append('}');
            return sb.toString();
        }
    }

    public class PooledNode {
        private String address;
        private AtomicInteger connectionsCorrupted;
        private AtomicInteger connectionsCreated;
        private AtomicInteger connectionsDestroyed;

        public PooledNode(String address) {
            this.address = address;
            connectionsCorrupted = new AtomicInteger();
            connectionsCreated = new AtomicInteger();
            connectionsDestroyed = new AtomicInteger();
        }

        public void decommission() {

        }

        public String getAddress() {
            return address;
        }

        public int getNumActive() {
            return pool.getNumActive(address);
        }

        public int getNumIdle() {
            return pool.getNumIdle(address);
        }

        void reportConnectionCorrupted() {
            connectionsCorrupted.incrementAndGet();
        }

        public int getConnectionsCorrupted() {
            return connectionsCorrupted.get();
        }

        void reportConnectionCreated() {
            connectionsCreated.incrementAndGet();
        }

        public int getConnectionsCreated() {
            return connectionsCreated.get();
        }

        void reportConnectionDestroyed() {
            connectionsDestroyed.incrementAndGet();
        }

        public int getConnectionsDestroyed() {
            return connectionsDestroyed.get();
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
    }

    private class ConnectionFactory extends BaseKeyedPoolableObjectFactory {
        @Override
        public Object makeObject(Object key) throws Exception {
            String nodeAddress = (String) key;
            logger.debug("Making new connection for node '{}:{}'", nodeAddress, cluster.getConnectionConfig().getThriftPort());
            PooledConnection connection = new PooledConnection(
                    new Cluster.Node(nodeAddress, cluster.getConnectionConfig()), getKeyspace()
            );
            connection.open();

            reportConnectionCreated(nodeAddress);

            return connection;
        }

        @Override
        public void destroyObject(Object key, Object obj) throws Exception {
            String nodeAddress = (String) key;
            logger.debug("Destroying connection for node '{}:{}'", nodeAddress, cluster.getConnectionConfig().getThriftPort());

            ((PooledConnection) obj).close();

            reportConnectionDestroyed(nodeAddress);
        }

        @Override
        public boolean validateObject(Object key, Object obj) {
            PooledConnection connection = (PooledConnection) obj;
            if (connection.isCorrupt() || !connection.isOpen()) {
                return false;
            } else {
                return true;
            }
        }

        @Override
        public void activateObject(Object key, Object obj) throws Exception {
        }

        @Override
        public void passivateObject(Object key, Object obj) throws Exception {
            String nodeAddress = (String) key;

            reportConnectionCorrupted(nodeAddress);
        }
    }

    public static interface INodeSelectionPolicy {
        PooledNode select(CommonsBackedPool pool, Map<String, PooledNode> nodes);
    }
}
