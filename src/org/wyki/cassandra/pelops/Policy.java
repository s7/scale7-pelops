package org.wyki.cassandra.pelops;

/**
 * Policy object that controls behavior and default values used by a particular Pelops connection
 * pool and related operations.
 * 
 * @author dominicwilliams
 *
 */
public class Policy {
	
	int minCachedConnectionsPerNode = 50;
	int targetConnectionsPerNode = 100;
	int maxConnectionsPerNode = 1000;
	
	int minGetConnectionRetryDelay = 50;
	int maxGetConnectionRetryDelay = 4000;
	int maxGetConnectionRetryWait = 10000;
	
	int defaultTargetRefillCheckPause = 2500;
	
	int maxOpRetries = 3;
	
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
	
	public int getMaxOpRetries() {
		return maxOpRetries;
	}
}
