package org.scale7.cassandra.pelops;

import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.TokenRange;
import org.scale7.cassandra.pelops.pool.DebuggingPool;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A heavy thread safe object that maintains a list of nodes in the cluster.  It's intended that
 * one instance of the class be available in the JVM per cluster.
 */
public class Cluster {
    private final Logger logger = SystemProxy.getLoggerFromFactory(DebuggingPool.class);

	private String[] nodes;
    private final IConnection.Config connectionConfig;
    private final INodeFilter nodeFilter;

    private boolean dynamicNodeDiscovery = false;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock lockRead = lock.readLock();
    private final Lock lockWrite = lock.writeLock();

    public Cluster(String nodes, int thriftPort) {
        this(splitAndTrim(nodes), new IConnection.Config(thriftPort, true, -1), false);
    }

    public Cluster(String nodes, int thriftPort, boolean dynamicNodeDiscovery) {
        this(splitAndTrim(nodes), new IConnection.Config(thriftPort, true, -1), dynamicNodeDiscovery);
    }

    public Cluster(String nodes, IConnection.Config connectionConfig, boolean dynamicNodeDiscovery) {
        this(splitAndTrim(nodes), connectionConfig, dynamicNodeDiscovery);
    }

    public Cluster(String[] nodes, IConnection.Config connectionConfig, boolean dynamicNodeDiscovery) {
        this(nodes, connectionConfig, dynamicNodeDiscovery, new AcceptAllNodeFilter());
	}

    public Cluster(String[] nodes, IConnection.Config connectionConfig, boolean dynamicNodeDiscovery, INodeFilter nodeFilter) {
        this.connectionConfig = connectionConfig;
        this.nodeFilter = nodeFilter;
        this.dynamicNodeDiscovery = dynamicNodeDiscovery;
        this.nodes = nodes;

        if (!dynamicNodeDiscovery) {
            logger.debug("Dynamic node discovery is disabled, using {} as a static list of nodes", Arrays.toString(nodes));
        } else {
            logger.debug("Dynamic node discovery is enabled, detecting initial list of nodes from {}", Arrays.toString(nodes));
            this.nodes = refreshInternal();
        }
	}

    public Cluster(String nodes, IConnection.Config connectionConfig, boolean dynamicNodeDiscovery, INodeFilter nodeFilter) {
        this(splitAndTrim(nodes), connectionConfig, dynamicNodeDiscovery, nodeFilter);
	}

    /**
     * Splits the provided string based on "," and trims leading or trailing whitespace for each host.
     * @param contactNodes the nodes
     * @return the split and trimmed nodes
     */
    private static String[] splitAndTrim(String contactNodes) {
        String[] splitContactNodes = contactNodes.split(",");
        for (int i = 0; i < splitContactNodes.length; i++) {
            splitContactNodes[i] = splitContactNodes[i].trim();
        }
        return splitContactNodes;
    }

    /**
     * Configuration used to open new connections.
     * @return the connection config
     */
    public IConnection.Config getConnectionConfig() {
        return connectionConfig;
    }

    /**
     * The current list of available nodes.
     * <p><b>Note</b>: avoid calling this method is a tight loop.
     * @return a copy of the current nodes
     */
    public Node[] getNodes() {
        try {
            lockRead.lock();
            Node[] nodes = new Node[this.nodes.length];
            for (int i = 0; i < this.nodes.length; i++) {
                String hostAddress = this.nodes[i];
                nodes[i] = new Node(hostAddress, getConnectionConfig());
            }

            return Arrays.copyOf(nodes, nodes.length);
        } finally {
            lockRead.unlock();
        }
    }

    /**
     * Refresh the current list of nodes.
     */
    public void refresh() {
        if (!dynamicNodeDiscovery)
            return;

        String[] latestNodes = refreshInternal();

        try {
            lockWrite.lock();
            nodes = latestNodes;
        } finally {
            lockWrite.unlock();
        }
    }

    /**
	 * Refresh the snapshot of the list of nodes currently believed to exist in the Cassandra cluster.
     * @return the list of nodes
	 */
	private String[] refreshInternal() {
		KeyspaceManager manager = Pelops.createKeyspaceManager(this);

        try {
            KeyspaceManager kspcMngr = Pelops.createKeyspaceManager(this);
            List<KsDef> keyspaces = kspcMngr.getKeyspaceNames();
            Iterator<KsDef> k = keyspaces.iterator();
            KsDef appKeyspace = null;
            while (k.hasNext()) {
                KsDef keyspace = k.next();
                if (!keyspace.getName().equals("system")) {
                    appKeyspace = keyspace;
                    break;
                }
            }
            if (appKeyspace == null)
                throw new Exception("Cannot obtain a node list from a ring mapping. No keyspaces are defined for this cluster.");

            logger.debug("Fetching nodes using keyspace '{}'", appKeyspace.getName());
            List<TokenRange> mappings = manager.getKeyspaceRingMappings(appKeyspace.getName());
            Set<String> clusterNodes = new HashSet<String>();
            for (TokenRange tokenRange : mappings) {
                List<String> endPointList = tokenRange.getEndpoints();
                clusterNodes.addAll(endPointList);
            }

            Iterator<String> iterator = clusterNodes.iterator();
            while (iterator.hasNext()) {
                String node = iterator.next();
                logger.debug("Checking node '{}' against node filter", node);
                if (!nodeFilter.accept(node)) {
                    logger.debug("Removing node '{}' as directed by node filter", node);
                    iterator.remove();
                }
            }

            String[] nodes = clusterNodes.toArray(new String[clusterNodes.size()]);
            logger.debug("Final set of refreshed nodes: {}", Arrays.toString(nodes));

            return nodes;
        } catch (Exception e) {
            logger.error("Failed to discover nodes dynamically.  See cause for details...", e);
            return null;
        }
    }

    /**
     * A filter used to determine which nodes should be used when {@link org.scale7.cassandra.pelops.Cluster#refresh()
     * refreshing}.  Implementations could potentially filter nodes that are in other data centers etc.
     */
    public static interface INodeFilter {
        boolean accept(String node);
    }

    /**
     * Default implementation that accepts all nodes.
     */
    public static class AcceptAllNodeFilter implements INodeFilter {
        @Override
        public boolean accept(String node) {
            return true;
        }
    }

    /**
     * Represents a node in the cluster.
     */
    public static class Node {
        private final String address;
        private final IConnection.Config config;

        public Node(String address, IConnection.Config config) {
            this.address = address;
            this.config = config;
        }

        public String getAddress() {
            return address;
        }

        public IConnection.Config getConfig() {
            return config;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Node node = (Node) o;

            if (!address.equals(node.address)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return address.hashCode();
        }

        @Override
        public String toString() {
            return address + ":" + config.getThriftPort();
        }
    }
}
