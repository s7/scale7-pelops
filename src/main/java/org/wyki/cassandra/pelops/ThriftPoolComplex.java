package org.wyki.cassandra.pelops;

import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.thrift.Clock;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.wyki.concurrency.AutoResetEvent;
import org.wyki.networking.utility.NetworkAlgorithms;
import org.wyki.portability.SystemProxy;

/**
 * Provides intelligent pooling of Thrift connections to the Cassandra cluster including balancing the load
 * created by actual client activity. This is achieved by balancing those connections actually engaged in writing
 * requests or reading responses evenly across the cluster's nodes. Later versions may also poll cluster nodes to
 * detect response times in order to further improve connection distribution.
 *
 * @author dominicwilliams
 *
 */
public class ThriftPoolComplex extends ThriftPoolAbstract {

	private static final Logger logger = SystemProxy.getLoggerFromFactory(ThriftPoolComplex.class);

    /**
	 * Get a Cassandra connection to the least loaded node represented in the connection pool.
	 * @return						A connection to Cassandra
	 */
	@Override
    public Connection getConnection() throws Exception {
		return getConnectionExcept(null);
	}

    /**
     * Constructs a pool instance suitable for performing management operations.
     * @param contactNodes the nodes to contact
     * @param defaultPort the port to content the nodes on (9160)
     * @param poolPolicy the pool policy
     * @param generalPolicy the general pelops policy
     */
	public ThriftPoolComplex(String[] contactNodes, int defaultPort, Policy poolPolicy, GeneralPolicy generalPolicy) {
        this(contactNodes, defaultPort, null, false, poolPolicy, generalPolicy);
	}

    /**
     * Constructs a pool instance.
     * Note: unless you are performing management options the keyspace should be provided.
     * @param contactNodes the nodes to contact
     * @param defaultPort the port to content the nodes on (9160)
     * @param keyspace the keyspace to use (note: as of 0.7.0 this is basically a required parameter)
     * @param dynamicNodeDiscovery if nodes should be dynamically added and removed
     * @param poolPolicy the pool policy
     * @param generalPolicy the general pelops policy
     */
	public ThriftPoolComplex(String[] contactNodes, int defaultPort, String keyspace, boolean dynamicNodeDiscovery, Policy poolPolicy, GeneralPolicy generalPolicy) {
		this.defaultPort = defaultPort;
        this.generalPolicy = generalPolicy;
        pool = new MultiNodePool();
		this.keyspace = keyspace;
		this.poolPolicy = poolPolicy;
		for (String node : contactNodes)
			touchNodeContext(node);
		if (dynamicNodeDiscovery)
			clusterWatcherExec.execute(clusterWatcher);
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
    public Connection getConnectionExcept(String notNode) throws Exception {
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
				Connection conn = leastLoaded.getConnection();
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
				Connection conn = leastLoaded.getConnection();
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
	@Override
    public ThriftPoolPolicy getPoolPolicy() {
		return poolPolicy;
	}

    /**
     * Get the current policy in force, which controls the general behavior of pelops.
     * @return the current policy
     */
    @Override
    public GeneralPolicy getGeneralPolicy() {
        return generalPolicy;
    }

    @Override
    public String getKeyspace() {
        return keyspace;
    }

    @SuppressWarnings("serial")
	class MultiNodePool extends ConcurrentHashMap<String, NodeContext> {}

	private Policy poolPolicy;
	private final MultiNodePool pool;
	private final int defaultPort;
    private GeneralPolicy generalPolicy;
    private final String keyspace;
	private ExecutorService clusterWatcherExec = Executors.newSingleThreadExecutor();
    private AtomicBoolean isShutdown = new AtomicBoolean(false);


	private void touchNodeContext(String node) {
			NodeContext newContext = new NodeContext(node, keyspace);
			if (pool.putIfAbsent(node, newContext) == null)
				newContext.init();
	}

	private Runnable clusterWatcher = new Runnable () {

		@Override
		public void run() {
			while (true) {
				// Update cluster node contexts
				Metrics metrics = createMetrics();
				try {
					// Use key range mappings to derive list of available nodes in cluster
					HashSet<String> clusterNodes = new HashSet<String>();
					List<TokenRange> mappings = metrics.getKeyspaceRingMappings(keyspace);
					for (TokenRange tokenRange : mappings) {
						List<String> endPointList = tokenRange.getEndpoints();
						clusterNodes.addAll(endPointList);
					}
					// Adjust our list of node contexts accordingly
					for (String node : clusterNodes)
						touchNodeContext(node);
				} catch (Exception e) {
                    SystemProxy.getLoggerFromFactory(getClass()).error(e.getMessage(), e);
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
	public class ConnectionComplex implements Connection {
		private final NodeContext nodeContext;
		private final TTransport transport;
		private final TProtocol protocol;
		private final Client client;
		int nodeSessionId = 0;

		ConnectionComplex(NodeContext nodeContext, int port) throws SocketException, TException, InvalidRequestException {
			this.nodeContext = nodeContext;
            TSocket socket = new TSocket(nodeContext.node, port);
            transport = poolPolicy.isFramedTransportRequired() ? new TFramedTransport(socket) : socket;
			protocol = new TBinaryProtocol(transport);
			socket.getSocket().setKeepAlive(true);
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
			return nodeContext.node;
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
			nodeContext.onConnectionRelease(this, afterException);
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

                if (nodeContext.keyspace != null) {
                    try {
                        client.set_keyspace(nodeContext.keyspace);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                }
			} catch (TTransportException e) {
                logger.error(e.getMessage(), e);
				return false;
			}
			return true;
		}

        /**
         * Close the connection.
         */
		@Override
        public void close() {
			transport.close();
		}
    }

	@SuppressWarnings("serial")
	class ConnectionList extends ConcurrentLinkedQueue<Connection> {}

	class NodeContext {
		private final int MIN_CREATE_CONNECTION_BACK_OFF = 125;
		private final int MAX_CREATE_CONNECTION_BACK_OFF = 20000;
		private final String node;
        private String keyspace;
        private final AtomicInteger countInUse = new AtomicInteger(0);
		private final AtomicInteger countCached = new AtomicInteger(0);
		private final ConnectionList connCache = new ConnectionList();
		private ExecutorService refillExec = Executors.newSingleThreadExecutor();
		private AutoResetEvent refillNow = new AutoResetEvent(true);
		private final AtomicInteger sessionId = new AtomicInteger(0);

		NodeContext(String node, String keyspace) {
			this.node = node;
            this.keyspace = keyspace;
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

		Connection getConnection() {
			// Try to retrieve cached connection...
			try {
				Connection conn;
				while (true) {
					conn = connCache.poll();
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

		void onConnectionRelease(ConnectionComplex conn, boolean afterException) {
			// This connection is no longer in use
			countInUse.decrementAndGet();
			// Is this connection still open/reusable?
			if (!afterException) {
				// Do we want this connection?
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
				if (poolPolicy.isKillNodeConnsOnException())
					if (sessionId.compareAndSet(conn.nodeSessionId, sessionId.get()+1))
						killPooledConnectionsToNode(conn.nodeSessionId);
				// Since this connection has died, prompt refiller to check pool parameters
				refillNow.set();
			}
		}

		private Connection createConnection() {
			Connection conn;
			try {
				conn = new ConnectionComplex(this, defaultPort);
			} catch (Exception e) {
                logger.error(e.getMessage(), e);
				return null;
			}

			if (conn.open(sessionId.get()))
				return conn;

			return null;
		}

		private void killPooledConnectionsToNode(int nodeSessionId) {
			logger.warn("{} NodeContext killing all pooled connections for session {}", node, nodeSessionId);
			int killedCount = 0;
			Connection c = null;
			while ((c = connCache.poll()) != null) {
				countCached.decrementAndGet();
				c.close();
				killedCount++;
			}
			logger.trace("{} NodeContext killed {}", node, killedCount);
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
					for (Connection conn : connCache)
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
							Connection conn = createConnection();
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

    public static class Policy extends ThriftPoolPolicy {
        int minCachedConnectionsPerNode = 50;
        int targetConnectionsPerNode = 100;
        int maxConnectionsPerNode = 1000;

        int minGetConnectionRetryDelay = 50;
        int maxGetConnectionRetryDelay = 4000;
        int maxGetConnectionRetryWait = 10000;

        int defaultTargetRefillCheckPause = 2500;

        boolean killNodeConnsOnException = true;

        public Policy() {
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
