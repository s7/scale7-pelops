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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
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
public class ThriftPool {
	
	private static final Logger logger = SystemProxy.getLoggerFromFactory(ThriftPool.class);
	
	/**
	 * Create a <code>Selector</code> object.
	 * @param keyspace				The keyspace to operate on
	 * @return						A new <code>Selector</code> object
	 */
	public Selector createSelector(String keyspace) {
		return new Selector(this, keyspace);
	}
	
	/**
	 * Create a <code>Mutator</code> object using the current time as the operation time stamp. The <code>Mutator</code> object  
	 * must only be used to execute 1 mutation operation.
	 * @param keyspace				The keyspace to operate on
	 * @return						A new <code>Mutator</code> object
	 */
	public Mutator createMutator(String keyspace) {
		return new Mutator(this, keyspace);
	}
	
	/**
	 * Create a <code>Mutator</code> object with an arbitrary time stamp. The <code>Mutator</code> object
	 * must only be used to execute 1 mutation operation.
	 * @param keyspace				The keyspace to operate on
	 * @param timestamp				The default time stamp to use for operations
	 * @return						A new <code>Mutator</code> object
	 */
	public Mutator createMutator(String keyspace, long timestamp) {
		return new Mutator(this, keyspace, timestamp);
	}
	
	/**
	 * Create a <code>KeyDeletor</code> object using the current time as the operation time stamp.
	 * @param keyspace				The keyspace to operate on
	 * @return						A new <code>KeyDeletor</code> object
	 */
	public KeyDeletor createKeyDeletor(String keyspace) {
		return new KeyDeletor(this, keyspace);
	}
	
	/**
	 * Create a <code>KeyDeletor</code> object with an arbitrary time stamp.
	 * @param keyspace				The keyspace to operate on
	 * @param timestamp				The default time stamp to use for operations
	 * @return						A new <code>KeyDeletor</code> object
	 */
	public KeyDeletor createKeyDeletor(String keyspace, long timestamp) {
		return new KeyDeletor(this, keyspace, timestamp);
	}
	
	/**
	 * Create a <code>Metrics</code> object for discovering information about the Cassandra cluster and its contained keyspaces.
	 * @return						A new <code>Metrics</code> object
	 */
	public Metrics createMetrics() {
		return new Metrics(this);
	}
	
	/**
	 * Get a Cassandra connection to the least loaded node represented in the connection pool.
	 * @return						A connection to Cassandra
	 */
	public Connection getConnection() throws Exception {
		return getConnectionExcept(null);
	}
	
	ThriftPool(String[] contactNodes, int defaultPort, boolean dynamicNodeDiscovery, String discoveryKeyspace, Policy policy) {
		this.defaultPort = defaultPort;
		pool = new MultiNodePool();
		this.discoveryKeyspace = discoveryKeyspace;
		this.policy = policy;
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
			int retryPause = NetworkAlgorithms.getBinaryBackoffDelay(failedAttempts, policy.getMinGetConnectionRetryDelay(), policy.getMaxGetConnectionRetryDelay());
			totalTimeWaiting += retryPause;
			if (totalTimeWaiting > policy.getMaxGetConnectionRetryWait()) {
				logger.error("Failed to return a Cassandra connection. If another back off then max waiting time exceeded {} > {}", totalTimeWaiting, policy.getMaxGetConnectionRetryWait());
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
	public void shutdown() {
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
	
	/**
	 * Get the current policy in force, which controls the behavioral parameters of the connection pool.
	 * @return							The current policy
	 */
	public Policy getPolicy() {
		return policy;
	}
	
	@SuppressWarnings("serial")
	class MultiNodePool extends ConcurrentHashMap<String, NodeContext> {}
	
	private Policy policy;
	private final MultiNodePool pool;
	private final int defaultPort;
	private final String discoveryKeyspace;
	private ExecutorService clusterWatcherExec = Executors.newSingleThreadExecutor();
			
	
	private void touchNodeContext(String node) {
			NodeContext newContext = new NodeContext(node);
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
					List<TokenRange> mappings = metrics.getKeyspaceRingMappings(discoveryKeyspace);
					for (TokenRange tokenRange : mappings) {
						List<String> endPointList = tokenRange.getEndpoints();
						clusterNodes.addAll(endPointList);
					}
					// Adjust our list of node contexts accordingly
					for (String node : clusterNodes)
						touchNodeContext(node);
				} catch (Exception e) {
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
	public class Connection {
		private final NodeContext nodeContext;
		private final TTransport transport;
		private final TProtocol protocol;
		private final Client client;
		
		Connection(NodeContext nodeContext, int port) throws SocketException {
			this.nodeContext = nodeContext;
			TSocket socket = new TSocket(nodeContext.node, port);
			transport = socket;
			protocol = new TBinaryProtocol(transport);
			socket.getSocket().setKeepAlive(true);
			client = new Client(protocol);
		}
				
		/**
		 * Get a reference to the Cassandra Thrift API
		 * @return					The raw Thrift interface
		 */
		public Client getAPI() {
			return client;
		}
		
		/**
		 * Get a string identifying the node
		 * @return					The IP or DNS address of the node
		 */
		public String getNode() {
			return nodeContext.node;
		}
				
		/**
		 * Flush the underlying transport connection used by Thrift. This is used to ensure all
		 * writes have been sent to Cassandra.
		 * @throws TTransportException
		 */
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
		public void release(boolean afterException) {
			nodeContext.onConnectionRelease(this, afterException);
		}
		
		boolean open() {
			try {
				transport.open();
			} catch (TTransportException e) {
				e.printStackTrace();
				return false;
			}
			return true;
		}
		
		boolean isOpen() {
			return transport.isOpen();
		}		
	}
	
	@SuppressWarnings("serial")
	class ConnectionList extends ConcurrentLinkedQueue<Connection> {}
	
	class NodeContext {
		private final int MIN_CREATE_CONNECTION_BACK_OFF = 125;
		private final int MAX_CREATE_CONNECTION_BACK_OFF = 20000;
		private final String node;
		private final AtomicInteger countInUse = new AtomicInteger(0);
		private final AtomicInteger countCached = new AtomicInteger(0);
		private final ConnectionList connCache = new ConnectionList();
		private ExecutorService refillExec = Executors.newSingleThreadExecutor();
		private AutoResetEvent refillNow = new AutoResetEvent(true);
		
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

		void onConnectionRelease(Connection conn, boolean afterException) {
			// This connection is no longer in use
			countInUse.decrementAndGet();
			// Is this connection still open/reusable?
			if (conn.isOpen() && !afterException) {
				// Do we want this connection?
				if ((countInUse.get() + countCached.get()) < policy.getTargetConnectionsPerNode()) {
					connCache.add(conn);
					countCached.incrementAndGet();
				}
			} else
				// Since this connection has died, prompt refiller to check pool parameters
				refillNow.set();
		}
		
		private Connection createConnection() {
			Connection conn;
			try {
				conn = new Connection(this, defaultPort);
			} catch (SocketException e) {
				e.printStackTrace();
				return null;
			}
			
			if (conn.open())
				return conn;
			
			return null;
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
							refillNow.waitOne(policy.getDefaultTargetRefillCheckPause(), TimeUnit.MILLISECONDS);
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
					if (policy.getMaxConnectionsPerNode() == -1 || (countInUse.get() + countCached.get()) < policy.getMaxConnectionsPerNode()) {
						// Do we actually want to create any more connections?
						while (countCached.get() < policy.getMinCachedConnectionsPerNode() ||
								(countInUse.get() + countCached.get()) < policy.getTargetConnectionsPerNode()) {
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
}
