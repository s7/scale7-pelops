package org.wyki.cassandra.pelops;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.thrift.Clock;
import org.slf4j.Logger;
import org.wyki.portability.SystemProxy;

public class Pelops {

	private static final Logger logger = SystemProxy.getLoggerFromFactory(Pelops.class);
	
	private static ConcurrentHashMap<String, IThriftPool> poolMap = new ConcurrentHashMap<String, IThriftPool>();
	
	/**
	 * Add a new Thrift connection pool and give it a name. The name is later used to identify the pool from which to request
	 * a connection when creating operands such as <code>Mutator</code> and <code>Selector</code>. A pool maintains connections to
	 * a specific Cassandra cluster instance. Typically a pool is created for each Cassandra cluster that must be accessed.
	 * @param poolName				A name used to reference the pool e.g. "MainDatabase" or "LucandraIndexes"
	 * @param contactNodes			An array of IP or DNS addresses identifying "known" nodes in this Cassandra cluster
	 * @param defaultPort			The port upon which Cassandra instances in this cluster will be listening
	 * @param dynamicNodeDiscovery  Whether the pool should dynamically discover the cluster node members using describe_ring(). Note: due to a Cassandra bug this cannot be used on single node clusters or clusters with a replication factor of 1 prior to 6.1
	 * @param discoveryKeyspace		If dynamic cluster node discovery is on, a default keyspace to be queried using describe_ring() to discover the nodes
	 * @param policy				Policy object controlling behavior
     * @deprecated Use the {@link #addPool(String, String[], int, boolean, String, GeneralPolicy, org.wyki.cassandra.pelops.ThriftPoolComplex.Policy)} method instead
	 */
	public static void addPool(String poolName, String[] contactNodes, int defaultPort, boolean dynamicNodeDiscovery,
                               String discoveryKeyspace, Policy policy) {
        GeneralPolicy generalPolicy = new GeneralPolicy();
        generalPolicy.setMaxOpRetries(policy.getMaxOpRetries());

        ThriftPoolComplex.Policy poolPolicy = new ThriftPoolComplex.Policy();
        poolPolicy.setKillNodeConnsOnException(policy.getKillNodeConnsOnException());
        poolPolicy.setMaxConnectionsPerNode(policy.getMaxConnectionsPerNode());
        poolPolicy.setMinCachedConnectionsPerNode(policy.getMinCachedConnectionsPerNode());
        poolPolicy.setTargetConnectionsPerNode(policy.getTargetConnectionsPerNode());

		addPool(poolName, contactNodes, defaultPort, dynamicNodeDiscovery, discoveryKeyspace, generalPolicy, poolPolicy);
	}

    /**
	 * Add a new Thrift connection pool and give it a name. The name is later used to identify the pool from which to request
	 * a connection when creating operands such as <code>Mutator</code> and <code>Selector</code>. A pool maintains connections to
	 * a specific Cassandra cluster instance. Typically a pool is created for each Cassandra cluster that must be accessed.
	 * @param poolName				A name used to reference the pool e.g. "MainDatabase" or "LucandraIndexes"
	 * @param contactNodes			An array of IP or DNS addresses identifying "known" nodes in this Cassandra cluster
	 * @param defaultPort			The port upon which Cassandra instances in this cluster will be listening
	 * @param dynamicNodeDiscovery  Whether the pool should dynamically discover the cluster node members using describe_ring(). Note: due to a Cassandra bug this cannot be used on single node clusters or clusters with a replication factor of 1 prior to 6.1
	 * @param discoveryKeyspace		If dynamic cluster node discovery is on, a default keyspace to be queried using describe_ring() to discover the nodes
     * @param generalPolicy		    General pelops policy object controlling behavior
     * @param poolPolicy            Pool policy object controlling pool behavior
	 */
	public static void addPool(String poolName, String[] contactNodes, int defaultPort, boolean dynamicNodeDiscovery,
                               String discoveryKeyspace, GeneralPolicy generalPolicy, ThriftPoolComplex.Policy poolPolicy) {
		ThriftPoolComplex newPool = new ThriftPoolComplex(
                contactNodes, defaultPort, discoveryKeyspace, dynamicNodeDiscovery, poolPolicy, generalPolicy
        );
        addPool(poolName, newPool);
	}

    /**
     * Add an already instantiated instance of {@link IThriftPool} to pelops.
     * @param poolName A name used to reference the pool e.g. "MainDatabase" or "LucandraIndexes"
     * @param thriftPool an instance of the {@link IThriftPool} interface
     */
    public static void addPool(String poolName, IThriftPool thriftPool) {
        logger.info("Pelops adds new pool {}", poolName);
        poolMap.put(poolName, thriftPool);
    }
	
	/**
	 * Shutdown Pelops. This proceeds by shutting down all connection pools.
	 */
	public static void shutdown() {
		logger.info("Pelops starting to shutdown...");
		for (IThriftPool pool : poolMap.values())
			pool.shutdown();
		logger.info("Pelops has shutdown");
	}
	
	/**
	 * Create a <code>Selector</code> object.
	 * @param poolName				The name of the connection pool to use (this determines the Cassandra database cluster)
	 * @return						A new <code>Selector</code> object
	 */
	public static Selector createSelector(String poolName) {
		return poolMap.get(poolName).createSelector();
	}
	
	/**
	 * Create a <code>Mutator</code> object using the current time as the operation time stamp. The <code>Mutator</code> object  
	 * must only be used to execute 1 mutation operation.
	 * @param poolName				The name of the connection pool to use (this determines the Cassandra database cluster)
	 * @return						A new <code>Mutator</code> object
	 */
	public static Mutator createMutator(String poolName) {
		return poolMap.get(poolName).createMutator();
	}
	
	/**
	 * Create a <code>Mutator</code> object with an arbitrary time stamp. The <code>Mutator</code> object
	 * must only be used to execute 1 mutation operation.
	 * @param poolName				The name of the connection pool to use (this determines the Cassandra database cluster)
	 * @param timestamp				The default time stamp to use for operations
	 * @return						A new <code>Mutator</code> object
	 */
	public static Mutator createMutator(String poolName, long timestamp) {
		return poolMap.get(poolName).createMutator(timestamp);
	}

	/**
	 * Create a <code>Mutator</code> object with an arbitrary time stamp. The <code>Mutator</code> object
	 * must only be used to execute 1 mutation operation.
	 * @param poolName				The name of the connection pool to use (this determines the Cassandra database cluster)
	 * @param clock				    The default clock instance to use for operations
	 * @return						A new <code>Mutator</code> object
	 */
	public static Mutator createMutator(String poolName, Clock clock) {
		return poolMap.get(poolName).createMutator(clock);
	}
	
	/**
	 * Create a <code>KeyDeletor</code> object using the current time as the operation time stamp.
	 * @param poolName				The name of the connection pool to use (this determines the Cassandra database cluster)
	 * @return						A new <code>KeyDeletor</code> object
	 */
	public static KeyDeletor createKeyDeletor(String poolName) {
		return poolMap.get(poolName).createKeyDeletor();
	}
	
	/**
	 * Create a <code>KeyDeletor</code> object with an arbitrary time stamp.
	 * @param poolName				The name of the connection pool to use (this determines the Cassandra database cluster)
	 * @param timestamp				The default time stamp to use for operations
     * @return						A new <code>KeyDeletor</code> object
	 */
	public static KeyDeletor createKeyDeletor(String poolName, long timestamp) {
		return poolMap.get(poolName).createKeyDeletor(timestamp);
	}

	/**
	 * Create a <code>KeyDeletor</code> object with an arbitrary time stamp.
	 * @param poolName				The name of the connection pool to use (this determines the Cassandra database cluster)
	 * @param clock				    The default clock instance to use for operations
     * @return						A new <code>KeyDeletor</code> object
	 */
	public static KeyDeletor createKeyDeletor(String poolName, Clock clock) {
		return poolMap.get(poolName).createKeyDeletor(clock);
	}
	
	/**
	 * Create a <code>Metrics</code> object for discovering information about the Cassandra cluster and its contained keyspaces.
	 * @param poolName				The name of the connection pool to use (this determines the Cassandra database cluster)
	 * @return						A new <code>Metrics</code> object
	 */
	public static Metrics createMetrics(String poolName) {
		return poolMap.get(poolName).createMetrics();
	}

	/**
	 * Create an instance of {@link org.wyki.cassandra.pelops.Management} that can be used to batch several management
     * operation together.  It's very important that the {@link Management#release()} method is called after you have
     * finished with the instance.
	 * @param poolName				The name of the connection pool to use (this determines the Cassandra database cluster)
	 * @return						The management instance
	 */
	public static Management createManagement(String poolName) {
		return poolMap.get(poolName).createManagement();
	}

    /**
	 * Create an instance of {@link KeyspaceManagement} that can be used to batch several management
     * operation together.  It's very important that the {@link KeyspaceManagement#release()} method is called after you have 
     * finished with the instance.
	 * @param poolName				The name of the connection pool to use (this determines the Cassandra database cluster)
     * @return						The keyspace management instance
	 */
    public static KeyspaceManagement createKeyspaceManagement(String poolName) {
		return poolMap.get(poolName).createKeyspaceManagement();
	}
	
	/**
	 * Get a direct reference to a DbConnPool. This should never be needed while using Pelops's <code>Mutator</code> and
	 * <code>Selector</code> in normal usage. The reason this function is provided, is so that the Pelops connection pooling system can be
	 * used in conjunction with other systems such as Lucandra. 
	 * @param poolName				The name of the pool
	 * @return						A direct reference to the specified pool
	 */
	public static IThriftPool getDbConnPool(String poolName) {
		return poolMap.get(poolName);
	}
}
