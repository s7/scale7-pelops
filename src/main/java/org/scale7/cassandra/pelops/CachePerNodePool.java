package org.scale7.cassandra.pelops;

import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.scale7.concurrency.AutoResetEvent;
import org.scale7.networking.utility.NetworkAlgorithms;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

/**
 * Provides intelligent pooling of Thrift connections to the Cassandra cluster including balancing the load
 * created by actual client activity. This is achieved by balancing those connections actually engaged in writing
 * requests or reading responses evenly across the cluster's nodes. Later versions may also poll cluster nodes to
 * detect response times in order to further improve connection distribution.
 *
 * @author dominicwilliams
 *
 */
public class CachePerNodePool extends ThriftPoolBase {

	private static final Logger logger = SystemProxy.getLoggerFromFactory(CachePerNodePool.class);

	private Cluster cluster;
	private Policy poolPolicy;
	private final MultiNodePool pool;
    private OperandPolicy generalPolicy;
    private final String keyspace;
	private ExecutorService clusterWatcherExec = Executors.newSingleThreadExecutor();
    private AtomicBoolean isShutdown = new AtomicBoolean(false);

    /**
	 * Get a Cassandra connection to the least loaded node represented in the connection pool.
	 * @return						A connection to Cassandra
	 */
	@Override
    public IConnection getConnection() throws Exception {
		return getConnectionExcept(null);
	}

    /**
     * Constructs a pool instance.
     * Note: unless you are performing management options the keyspace should be provided.
     * @param contactNodes the nodes to contact
     * @param defaultPort the port to content the nodes on (9160)
     * @param keyspace the keyspace to use (note: as of 0.7.0 this is basically a required parameter)
     * @param generalPolicy the general pelops policy
     * @param poolPolicy the pool policy
     */
	public CachePerNodePool(Cluster cluster, String keyspace, OperandPolicy generalPolicy, Policy poolPolicy) {
        this.cluster = cluster;
        this.generalPolicy = generalPolicy;
        pool = new MultiNodePool();
		this.keyspace = keyspace;
		this.poolPolicy = poolPolicy;
		String[] nodesSnapshot = cluster.getCurrentNodesSnapshot();
		for (String node : nodesSnapshot)
			touchNodeContext(node);
		if (poolPolicy.getDynamicNodeDiscovery())
			clusterWatcherExec.execute(clusterWatcher);
		else
			logger.warn("Dynamic node discovery is false. Pelops will not automatically discover nodes added to the cluster, or nodes that have not been specified");
	}

	/**
	 * Get a Cassandra connection to the least loaded node represented in the connection pool.
	 * If specified, a specified node is avoided if any other nodes are available. This is useful if a
	 * node has just failed for a particular operation and it wishes to try another.
	 * @param notNode				A node to try and avoid, for example because using it just failed
	 * @return						A connection to a Cassandra
	 * @throws Exception
	 */
	@Override
    public IConnection getConnectionExcept(String notNode) throws Exception {
		// Create a list of nodes we have already tried, and therefore should avoid in preference
		// to trying new nodes.
		List<String> triedNodes = null;
		if (notNode != null) {
			triedNodes = new ArrayList<String>(16);
			triedNodes.add(notNode);
		}
		// Make a finite number of attempts to get a connection from the pool
		int failedAttempts = 0;
		int totalTimeWaiting = 0;
		while (true) {
			// Get the list of node contexts we are currently maintaining
			Collection<NodeContext> nodeContexts = pool.values();
			while (true) {
				// Choose the least loaded "available" node that has not been tried.
				NodeContext leastLoaded = null;
				for (NodeContext nodeContext : nodeContexts) {
					if (nodeContext.isAvailable()) {
						if (triedNodes == null || !triedNodes.contains(nodeContext.node))
							if (leastLoaded == null || leastLoaded.getNodeLoadIndex() >= nodeContext.getNodeLoadIndex())
								leastLoaded = nodeContext;
					}
				}
				// If we could not find an available untried node then break out and try any node
				if (leastLoaded == null)
					break;
				// otherwise, try to return a connection from this least loaded untried node
				IConnection conn = leastLoaded.getConnection();
				if (conn != null)
					return conn;
				// That node couldn't give us a connection, so loop to try and find another untried node
				if (triedNodes == null)
					triedNodes = new ArrayList<String>(16);
				triedNodes.add(leastLoaded.node);
			}
			// One attempt to get a connection to a node now. Doesn't matter if we tried before.
			NodeContext leastLoaded = null;
			for (NodeContext nodeContext : nodeContexts) {
				if (nodeContext.isAvailable()) {
					if (leastLoaded == null || leastLoaded.getNodeLoadIndex() >= nodeContext.getNodeLoadIndex())
						leastLoaded = nodeContext;
				}
			}
			if (leastLoaded != null) {
				IConnection conn = leastLoaded.getConnection();
				if (conn != null)
					return conn;
			}
			// Nope, that didn't work so need to back off and try again in a moment
			logger.warn("Unable to find a node to connect to. Backing off...");
			failedAttempts++;
			int retryPause = NetworkAlgorithms.getBinaryBackoffDelay(failedAttempts, poolPolicy.getMinGetConnectionRetryDelay(), poolPolicy.getMaxGetConnectionRetryDelay());
			totalTimeWaiting += retryPause;
			if (totalTimeWaiting > poolPolicy.getMaxGetConnectionRetryWait()) {
				logger.error("Failed to return a Cassandra connection. If another back off then max waiting time exceeded {} > {}", totalTimeWaiting, poolPolicy.getMaxGetConnectionRetryWait());
				throw new Exception("No Cassandra nodes are available");
			}
			// Sleep awhile
			Thread.sleep(retryPause);
		}
	}

    /**
	 * Cleanly shutdown this pool and associated Thrift connections and operations.
	 * TODO wait until all in-use connections are returned to the pool before exiting.
	 */
	@Override
    public void shutdown() {
        if (isShutdown.compareAndSet(false, true)) {
            // Stop watching the cluster
            clusterWatcherExec.shutdownNow();
            // Shutdown the cluster node contexts
            Collection<NodeContext> nodeContexts = pool.values();
            for (NodeContext nodeContext : nodeContexts)
                nodeContext.shutdown();
            for (NodeContext nodeContext : nodeContexts)
                nodeContext.waitShutdown();
            try {
                clusterWatcherExec.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
	}

    /**
	 * Get the current policy in force, which controls the behavioral parameters of the connection pool.
	 * @return							The current policy
	 */
    public Policy getPoolPolicy() {
		return poolPolicy;
	}

    /**
     * Get the current policy in force, which controls the general behavior of pelops.
     * @return the current policy
     */
    @Override
    public OperandPolicy getOperandPolicy() {
        return generalPolicy;
    }

    @Override
    public String getKeyspace() {
        return keyspace;
    }

    @SuppressWarnings("serial")
	class MultiNodePool extends ConcurrentHashMap<String, NodeContext> {}


	private void touchNodeContext(String node) {
			NodeContext newContext = new NodeContext(node);
			if (pool.putIfAbsent(node, newContext) == null)
				newContext.init();
	}

	private Runnable clusterWatcher = new Runnable () {

		@Override
		public void run() {
			while (true) {
				try {
					cluster.refreshNodesSnapshot();
					String[] clusterNodes = cluster.getCurrentNodesSnapshot();
					for (String node : clusterNodes)
						touchNodeContext(node);
				} catch (Exception e) {
					logger.warn("Cluster watcher process encountered error while refreshing snapshot", e.getMessage());
					e.printStackTrace();
				}
				// Sleep awhile
				try {
					Thread.sleep(60000);
				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
					return;
				}
			}
		}

	};

	/**
	 * Encapsulates a connection to a Cassandra node.
	 *
	 * @author dominicwilliams
	 *
	 */
	public class Connection implements IConnection {
        private String node;
        private String keyspace;
        private ConnectionReleaseHandler releaseHandler;
        private final TTransport transport;
		private final TProtocol protocol;
		private final Client client;
		int nodeSessionId = 0;

		Connection(String node, int port, String keyspace, ConnectionReleaseHandler releaseHandler) throws SocketException, TException, InvalidRequestException {
            this.node = node;
            this.keyspace = keyspace;
            this.releaseHandler = releaseHandler;
            TSocket socket = new TSocket(node, port);
            transport = cluster.isFramedTransportRequired() ? new TFramedTransport(socket) : socket;
			protocol = new TBinaryProtocol(transport);
			//socket.getSocket().setKeepAlive(true);
			client = new Client(protocol);
		}

		/**
		 * Get a reference to the Cassandra Thrift API
		 * @return					The raw Thrift interface
		 */
		@Override
        public Client getAPI() {
			return client;
		}

		/**
		 * Get a string identifying the node
		 * @return					The IP or DNS address of the node
		 */
		@Override
        public String getNode() {
			return node;
		}

		/**
		 * Flush the underlying transport connection used by Thrift. This is used to ensure all
		 * writes have been sent to Cassandra.
		 * @throws TTransportException if an error occurs while attempting to flush
		 */
		@Override
        public void flush() throws TTransportException {
			transport.flush();
		}

		/**
		 * Release a <code>Connection</code> that has previously been taken from the pool. Specify whether
		 * an exception has been thrown during usage of the connection. If an exception has been thrown, the
		 * connection will not re-used since it may be corrupted (for example, it may contain partially written
		 * data that disrupts the serialization of the Thrift protocol) however it is remains essential that all
		 * connection objects are released.
		 * @param afterException		Whether a connection was thrown during usage
		 */
		@Override
        public void release(boolean afterException) {
            releaseHandler.release(this, afterException);
		}

		@Override
        public boolean isOpen() {
			return transport.isOpen();
		}

        /**
         * Opens a connection.
         * @param nodeSessionId the node session Id
         * @return true if the connection was opened, otherwise false
         */
		@Override
        public boolean open(int nodeSessionId) {
			try {
				transport.open();
				this.nodeSessionId = nodeSessionId;

                if (keyspace != null) {
                    try {
                        client.set_keyspace(keyspace);
                    } catch (Exception e) {
                        logger.warn("Failed to set keyspace on client.  See cause for details...", e);
                        return false;
                    }
                }
			} catch (TTransportException e) {
                logger.error("Failed to open transport.  See cause for details...", e);
				return false;
			}
			return true;
		}

		@Override
		public int getSessionId() {
			return nodeSessionId;
		}

        /**
         * Close the connection.
         */
		@Override
        public void close() {
			transport.close();
		}
    }

    public interface ConnectionReleaseHandler {
        void release(Connection connection, boolean afterException);
    }

	@SuppressWarnings("serial")
	class ConnectionList extends ConcurrentLinkedQueue<IConnection> {}

	class NodeContext {
		private final int MIN_CREATE_CONNECTION_BACK_OFF = 125;
		private final int MAX_CREATE_CONNECTION_BACK_OFF = 20000;
		private final String node;
        private final AtomicInteger countInUse = new AtomicInteger(0);
		private final AtomicInteger countCached = new AtomicInteger(0);
		private final ConnectionList connCache = new ConnectionList();
		private ExecutorService refillExec = Executors.newSingleThreadExecutor();
		private AutoResetEvent refillNow = new AutoResetEvent(true);
		private final AtomicInteger sessionId = new AtomicInteger(0);

		NodeContext(String node) {
			this.node = node;
        }

		void init() {
			refillExec.execute(poolRefiller);
		}

		void shutdown() {
			refillExec.shutdownNow();
		}

		void waitShutdown() {
			try {
				refillExec.awaitTermination(10, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

		int getNodeLoadIndex() {
			return countInUse.get();
		}

		boolean isAvailable() {
			return countCached.get() > 0;
		}

		private Integer connCacheLock = new Integer(-1);

		IConnection getConnection() {
			// Try to retrieve cached connection...
			try {
				IConnection conn;
				while (true) {
					synchronized (connCacheLock) {
						conn = connCache.poll();
					}
					if (conn == null)
						return null;
					else
						countCached.decrementAndGet();

					if (conn.isOpen()) {
						countInUse.incrementAndGet();
						return conn;
					}
				}
			} finally {
				// Need to check whether pool refill needed
				refillNow.set();
			}
		}

		void onConnectionRelease(Connection conn, boolean networkException) {
			// This connection is no longer in use
			countInUse.decrementAndGet();
			// Is this connection still open/reusable?
			if (!networkException) {
				// Yes, we can keep this connection if we still want it!
				if (conn.isOpen() && (countInUse.get() + countCached.get()) < poolPolicy.getTargetConnectionsPerNode()) {
					connCache.add(conn);
					countCached.incrementAndGet();
				} else {
                    conn.close();
                }
			} else {
				// close connection
				conn.close();
				// kill all connections to this node?
				if (poolPolicy.isKillNodeConnsOnException()) {
					sessionId.incrementAndGet();
					purgeConnsCreatedToSession(conn.nodeSessionId);
				}
				// Since this connection has died, prompt refiller to check pool parameters
				refillNow.set();
			}
		}

		private IConnection createConnection() {
			IConnection conn;
			try {
				conn = new Connection(this.node, cluster.getThriftPort(), keyspace, new ConnectionReleaseHandler() {
                    @Override
                    public void release(Connection connection, boolean afterException) {
			            onConnectionRelease(connection, afterException);
                    }
                });
			} catch (Exception e) {
                logger.error(e.getMessage(), e);
				return null;
			}

			if (conn.open(sessionId.get()))
				return conn;

			return null;
		}

		private void purgeConnsCreatedToSession(int nodeSessionId) {
			logger.warn("{} NodeContext killing all pooled connections for session {}", node, nodeSessionId);
			int killedCount = 0;
			synchronized (connCacheLock) {
				Iterator<IConnection> i = connCache.iterator();
				while (i.hasNext()) {
					IConnection c = i.next();
					if (c.getSessionId() <= nodeSessionId) {
						i.remove();
						countCached.decrementAndGet();
						c.close();
						killedCount++;
					}
				}
			}
			logger.trace("{} NodeContext purged {}", node, killedCount);
		}

		private Runnable poolRefiller = new Runnable () {

			@Override
			public void run() {
				int failureCount = 0;
				int backOffDelay = 0;
				while (true) {
					try {
						if (failureCount == 0)
							// Wait for standard scavenger pause or signal
							refillNow.waitOne(poolPolicy.getDefaultTargetRefillCheckPause(), TimeUnit.MILLISECONDS);
						else
							// We are in back off mode so wait for current back off delay
							Thread.sleep(backOffDelay);

					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						return;
					}

					// Remove dead connections from waiting pool
					int foundDead = 0;
					for (IConnection conn : connCache)
						if (!conn.isOpen()) {
							countCached.decrementAndGet();
							connCache.remove(conn);
							foundDead++;
						}

					if (foundDead > 0)
						logger.trace("{} NodeContext discarded {} dead connections", node, foundDead);

					// Are we allowed to create any more connections?
					if (poolPolicy.getMaxConnectionsPerNode() == -1 || (countInUse.get() + countCached.get()) < poolPolicy.getMaxConnectionsPerNode()) {
						// Do we actually want to create any more connections?
						while (countCached.get() < poolPolicy.getMinCachedConnectionsPerNode() ||
								(countInUse.get() + countCached.get()) < poolPolicy.getTargetConnectionsPerNode()) {
							// Yup create new connection for cache
							IConnection conn = createConnection();
							if (conn == null) {
								// Connection error occurred. Calculate back off delay
								failureCount++;
								backOffDelay = NetworkAlgorithms.getBinaryBackoffDelay(
										failureCount, MIN_CREATE_CONNECTION_BACK_OFF, MAX_CREATE_CONNECTION_BACK_OFF);
								// Do back off...
								logger.debug("{} NodeContext failed to create connection. Successive failure {}. Backing off...", node, failureCount);
								break;
							}
							// We managed to create new connection
							failureCount = 0;
							// Add new connection to waiting cache
							countCached.incrementAndGet();
							connCache.add(conn);
						}
					}
					logger.trace("{} NodeContext has {} in-use connections", node, countInUse.get());
					logger.trace("{} NodeContext has {} cached connections", node, countCached.get());
				}
			}

		};
	}

    public static class Policy {

        public Policy() {
        }

    	boolean dynamicNodeDiscovery = false;

        int minCachedConnectionsPerNode = 50;
        int targetConnectionsPerNode = 100;
        int maxConnectionsPerNode = 1000;

        int minGetConnectionRetryDelay = 50;
        int maxGetConnectionRetryDelay = 4000;
        int maxGetConnectionRetryWait = 10000;

        int defaultTargetRefillCheckPause = 2500;

        boolean killNodeConnsOnException = true;

        public boolean getDynamicNodeDiscovery() {
        	return dynamicNodeDiscovery;
        }

        /**
         * If dynamic node discovery is switched on, Pelops will periodically attempt to discover the current
         * set of nodes comprising the complete cluster. This means that if new nodes have been bootstrapped,
         * it automatically starts directing operations to them even though initially they were not specified
         * in the list of contact nodes. If you use this option, you must make sure that your cluster nodes
         * are listening on addresses that you can reach e.g. as configured via <code>listen_address</code> in
         * their cassandra.yaml configuration (this option controls what node addresses are reported to Pelops).
         * @param dynamicNodeDiscovery				The value for dynamic node discovery feature
         */
        public void setDynamicNodeDiscovery(boolean dynamicNodeDiscovery) {
        	this.dynamicNodeDiscovery = dynamicNodeDiscovery;
        }

        /**
         * Set the minimum cached connections per node. These are held ready so that operations
         * that require them are not delayed while a connection is made.
         */
        public void setMinCachedConnectionsPerNode(int minCachedConnectionsPerNode) {
            this.minCachedConnectionsPerNode = minCachedConnectionsPerNode;
        }

        public int getMinCachedConnectionsPerNode() {
            return minCachedConnectionsPerNode;
        }

        /**
         * Set the total number of connections per node we would like to hold in hand. This includes
         * both in-use connections and cached connections.
         */
        public void setTargetConnectionsPerNode(int targetConnectionsPerNode) {
            this.targetConnectionsPerNode = targetConnectionsPerNode;
        }

        public int getTargetConnectionsPerNode() {
            return targetConnectionsPerNode;
        }

        /**
         * Set the maximum number of connections per node we will allow to exist. This includes both
         * in-use connections and cached connections. Pass -1 to allow unlimited connections.
         */
        public void setMaxConnectionsPerNode(int maxConnectionsPerNode) {
            this.maxConnectionsPerNode = maxConnectionsPerNode;
        }

        public int getMaxConnectionsPerNode() {
            return maxConnectionsPerNode;
        }

        public int getMinGetConnectionRetryDelay() {
            return minGetConnectionRetryDelay;
        }

        public int getMaxGetConnectionRetryDelay() {
            return maxGetConnectionRetryDelay;
        }

        public int getMaxGetConnectionRetryWait() {
            return maxGetConnectionRetryWait;
        }

        public int getDefaultTargetRefillCheckPause() {
            return defaultTargetRefillCheckPause;
        }

        public boolean isKillNodeConnsOnException() {
            return killNodeConnsOnException;
        }

        /**
         * On some networks, for some unknown reason, the Thrift TSocket is not able to detect when it
         * has been disconnected. The Cassandra node can be shutdown, and the TSocket still thinks
         * it is connected! The only way around this it seems is to remove all connections to the
         * node when the problem occurs.
         * TODO find out what is going on with Thrift
         */
        public void setKillNodeConnsOnException(boolean killNodeConnsOnException) {
            this.killNodeConnsOnException = killNodeConnsOnException;
        }
    }
}
