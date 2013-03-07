/*
 * The MIT License
 *
 * Copyright (c) 2011 Dominic Williams, Daniel Washusen and contributors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package org.scale7.cassandra.pelops;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.TokenRange;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

/**
 * A heavy thread safe object that maintains a list of nodes in the cluster.  It's intended that
 * one instance of the class be available in the JVM per cluster.
 *
 * <p><b>Note</b>: The timeout parameter on the various constructors refers to ALL thrift related operations.
 * See: https://issues.apache.org/jira/browse/CASSANDRA-959</p>
 */
public class Cluster {
    /**
     * The default number of milliseconds to wait for an operation to complete.
     */
    public static final int DEFAULT_TIMEOUT = 4000;

    private final Logger logger = SystemProxy.getLoggerFromFactory(Cluster.class);

	private String[] nodes;
    private final IConnection.Config connectionConfig;
    private final INodeFilter nodeFilter;

    private boolean dynamicNodeDiscovery = false;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock lockRead = lock.readLock();
    private final Lock lockWrite = lock.writeLock();

    /**
     * Creates a new cluster using the {@link #DEFAULT_TIMEOUT} with dynamic node discovery turned off.
     * @param nodes comma separated list of nodes
     * @param thriftPort the thrift port
     */
    public Cluster(String nodes, int thriftPort) {
        this(splitAndTrim(nodes), new IConnection.Config(thriftPort, true, DEFAULT_TIMEOUT), false);
    }

    /**
     * Creates a new cluster using the {@link #DEFAULT_TIMEOUT} with dynamic node discovery turned off.
     * @param nodes comma separated list of nodes
     * @param thriftPort the thrift port
     * @param sslTransportRequired  is SSL transport required
     * @param trustStorePath path to trust store
     * @param trustStorePassword password to the trust store
     */
    public Cluster(String nodes, int thriftPort,
                    boolean sslTransportRequired, String trustStorePath, String trustStorePassword) {
        this(splitAndTrim(nodes), new IConnection.Config(thriftPort, true, DEFAULT_TIMEOUT,
                    null,  sslTransportRequired, trustStorePath, trustStorePassword), false);
    }
    
    /**
     * Creates a new cluster using the {@link #DEFAULT_TIMEOUT} with dynamic node discovery turned off.
     * @param nodes comma separated list of nodes
     * @param thriftPort the thrift port
     * @param connectionAuthenticator for node connection authentication
     */
    public Cluster(String nodes, int thriftPort,IConnectionAuthenticator connectionAuthenticator) {
        this(splitAndTrim(nodes), new IConnection.Config(thriftPort, true, DEFAULT_TIMEOUT,connectionAuthenticator), false);
    }
    
    /**
     * Creates a new cluster using the {@link #DEFAULT_TIMEOUT} with dynamic node discovery turned off.
     * @param nodes comma separated list of nodes
     * @param thriftPort the thrift port
     * @param connectionAuthenticator for node connection authentication
     * @param sslTransportRequired  is SSL transport required
     * @param trustStorePath path to trust store
     * @param trustStorePassword password to the trust store
     */
    public Cluster(String nodes, int thriftPort,IConnectionAuthenticator connectionAuthenticator,
                    boolean sslTransportRequired, String trustStorePath, String trustStorePassword) {
        this(splitAndTrim(nodes), new IConnection.Config(thriftPort, true, DEFAULT_TIMEOUT,connectionAuthenticator,
                sslTransportRequired, trustStorePath, trustStorePassword), false);
    }

    /**
     * Creates a new cluster using the {@link #DEFAULT_TIMEOUT}.
     * @param nodes comma separated list of nodes
     * @param thriftPort the thrift port
     * @param dynamicNodeDiscovery true if nodes should be discovered dynamically
     */
    public Cluster(String nodes, int thriftPort, boolean dynamicNodeDiscovery) {
        this(splitAndTrim(nodes), new IConnection.Config(thriftPort, true, DEFAULT_TIMEOUT), dynamicNodeDiscovery);
    }

    /**
     * Creates a new cluster using the {@link #DEFAULT_TIMEOUT}.
     * @param nodes comma separated list of nodes
     * @param thriftPort the thrift port
     * @param dynamicNodeDiscovery true if nodes should be discovered dynamically
     * @param sslTransportRequired  is SSL transport required
     * @param trustStorePath path to trust store
     * @param trustStorePassword password to the trust store
     */
    public Cluster(String nodes, int thriftPort, boolean dynamicNodeDiscovery,
                    boolean sslTransportRequired, String trustStorePath, String trustStorePassword) {
        this(splitAndTrim(nodes), new IConnection.Config(thriftPort, true, DEFAULT_TIMEOUT,
                sslTransportRequired, trustStorePath, trustStorePassword), dynamicNodeDiscovery);
    }
    
    /**
     * Creates a new cluster using the {@link #DEFAULT_TIMEOUT}.
     * @param nodes comma separated list of nodes
     * @param thriftPort the thrift port
     * @param dynamicNodeDiscovery true if nodes should be discovered dynamically
     * @param connectionAuthenticator for node connection authentication
     */
    public Cluster(String nodes, int thriftPort, boolean dynamicNodeDiscovery,IConnectionAuthenticator connectionAuthenticator) {
        this(splitAndTrim(nodes), new IConnection.Config(thriftPort, true, DEFAULT_TIMEOUT,connectionAuthenticator), dynamicNodeDiscovery);
    }
    
    /**
     * Creates a new cluster using the {@link #DEFAULT_TIMEOUT}.
     * @param nodes comma separated list of nodes
     * @param thriftPort the thrift port
     * @param dynamicNodeDiscovery true if nodes should be discovered dynamically
     * @param connectionAuthenticator for node connection authentication
     * @param sslTransportRequired  is SSL transport required
     * @param trustStorePath path to trust store
     * @param trustStorePassword password to the trust store
     */
    public Cluster(String nodes, int thriftPort, boolean dynamicNodeDiscovery,IConnectionAuthenticator connectionAuthenticator,
                    boolean sslTransportRequired, String trustStorePath, String trustStorePassword) {
        this(splitAndTrim(nodes), new IConnection.Config(thriftPort, true, DEFAULT_TIMEOUT,connectionAuthenticator,
                sslTransportRequired, trustStorePath, trustStorePassword), dynamicNodeDiscovery);
    }

    /**
     * Creates a new cluster.
     * @param nodes comma separated list of nodes
     * @param thriftPort the thrift port
     * @param timeout the number of milliseconds thrift should wait to complete an operation (zero or less disables the timeout)
     * @param dynamicNodeDiscovery true if nodes should be discovered dynamically
     */
    public Cluster(String nodes, int thriftPort, int timeout, boolean dynamicNodeDiscovery) {
        this(splitAndTrim(nodes), new IConnection.Config(thriftPort, true, timeout), dynamicNodeDiscovery);
    }

    /**
     * Creates a new cluster.
     * @param nodes comma separated list of nodes
     * @param thriftPort the thrift port
     * @param timeout the number of milliseconds thrift should wait to complete an operation (zero or less disables the timeout)
     * @param dynamicNodeDiscovery true if nodes should be discovered dynamically
     * @param sslTransportRequired  is SSL transport required
     * @param trustStorePath path to trust store
     * @param trustStorePassword password to the trust store
     */
    public Cluster(String nodes, int thriftPort, int timeout, boolean dynamicNodeDiscovery,
                    boolean sslTransportRequired, String trustStorePath, String trustStorePassword) {
        this(splitAndTrim(nodes), new IConnection.Config(thriftPort, true, timeout,
                sslTransportRequired, trustStorePath, trustStorePassword), dynamicNodeDiscovery);
    }
    
    /**
     * Creates a new cluster.
     * @param nodes comma separated list of nodes
     * @param thriftPort the thrift port
     * @param timeout the number of milliseconds thrift should wait to complete an operation (zero or less disables the timeout)
     * @param dynamicNodeDiscovery true if nodes should be discovered dynamically
     * @param connectionAuthenticator for node connection authentication
     */
    public Cluster(String nodes, int thriftPort, int timeout, boolean dynamicNodeDiscovery,IConnectionAuthenticator connectionAuthenticator) {
        this(splitAndTrim(nodes), new IConnection.Config(thriftPort, true, timeout,connectionAuthenticator), dynamicNodeDiscovery);
    }
    
    /**
     * Creates a new cluster.
     * @param nodes comma separated list of nodes
     * @param thriftPort the thrift port
     * @param timeout the number of milliseconds thrift should wait to complete an operation (zero or less disables the timeout)
     * @param dynamicNodeDiscovery true if nodes should be discovered dynamically
     * @param connectionAuthenticator for node connection authentication
     * @param sslTransportRequired  is SSL transport required
     * @param trustStorePath path to trust store
     * @param trustStorePassword password to the trust store
     */
    public Cluster(String nodes, int thriftPort, int timeout, boolean dynamicNodeDiscovery,IConnectionAuthenticator connectionAuthenticator,
                    boolean sslTransportRequired, String trustStorePath, String trustStorePassword) {
        this(splitAndTrim(nodes), new IConnection.Config(thriftPort, true, timeout,connectionAuthenticator,
                sslTransportRequired, trustStorePath, trustStorePassword), dynamicNodeDiscovery);
    }

    /**
     * Creates a new cluster.
     * @param nodes comma separated list of nodes
     * @param connectionConfig the connection config
     * @param dynamicNodeDiscovery true if nodes should be discovered dynamically
     */
    public Cluster(String nodes, IConnection.Config connectionConfig, boolean dynamicNodeDiscovery) {
        this(splitAndTrim(nodes), connectionConfig, dynamicNodeDiscovery);
    }

    /**
     * Creates a new cluster.
     * @param nodes array of nodes
     * @param connectionConfig the connection config
     * @param dynamicNodeDiscovery true if nodes should be discovered dynamically
     */
    public Cluster(String[] nodes, IConnection.Config connectionConfig, boolean dynamicNodeDiscovery) {
        this(nodes, connectionConfig, dynamicNodeDiscovery, new AcceptAllNodeFilter());
	}

    /**
     * Creates a new cluster.
     * @param nodes array of nodes
     * @param connectionConfig the connection config
     * @param dynamicNodeDiscovery true if nodes should be discovered dynamically
     * @param nodeFilter used to filter nodes when dynamic node discovery is enabled
     */
    public Cluster(String[] nodes, IConnection.Config connectionConfig, boolean dynamicNodeDiscovery, INodeFilter nodeFilter) {
        this.connectionConfig = connectionConfig;
        this.nodeFilter = nodeFilter;
        this.dynamicNodeDiscovery = dynamicNodeDiscovery;
        
        // make sure there are no duplicates
        this.nodes = new HashSet<String>(Arrays.asList(nodes)).toArray(new String[nodes.length]);

        if (!dynamicNodeDiscovery) {
            logger.info("Dynamic node discovery is disabled, using {} as a static list of nodes", Arrays.toString(nodes));
        } else {
            logger.info("Dynamic node discovery is enabled, detecting initial list of nodes from {}", Arrays.toString(nodes));
            refresh();
        }
	}

    /**
     * Creates a new cluster.
     * @param nodes comma separated list of nodes
     * @param connectionConfig the connection config
     * @param dynamicNodeDiscovery true if nodes should be discovered dynamically
     * @param nodeFilter used to filter nodes when dynamic node discovery is enabled
     */
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

            return nodes;
        } finally {
            lockRead.unlock();
        }
    }

    /**
     * Refresh the current list of nodes.
     * @param keyspace optional keyspace name used to obtain the node ring
     */
    public void refresh(String keyspace) {
        if (!dynamicNodeDiscovery)
            return;

        String[] latestNodes;
        try {
            if (keyspace != null)
                latestNodes = refreshInternal(keyspace);
            else
                latestNodes = refreshInternal();
        } catch (Exception e) {
            logger.error("Failed to discover nodes dynamically, using existing list of nodes.  See cause for details...", e);
            return;
        }

        try {
            lockWrite.lock();
            nodes = latestNodes;
        } finally {
            lockWrite.unlock();
        }
    }

    /**
     * Refresh the current list of nodes.
     */
    public void refresh() {
        refresh(null);
    }

    /**
	 * Refresh the snapshot of the list of nodes currently believed to exist in the Cassandra cluster.
     * @return the list of nodes
	 */
	private String[] refreshInternal() throws Exception {
        KeyspaceManager kspcMngr = new ClusterKeyspaceManager(this);
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

        return refreshInternal(appKeyspace.getName());
    }

    private String[] refreshInternal(String keyspace) throws Exception {
        KeyspaceManager manager = Pelops.createKeyspaceManager(this);
        logger.debug("Fetching nodes using keyspace '{}'", keyspace);
        List<TokenRange> mappings = manager.getKeyspaceRingMappings(keyspace);
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

    public class ClusterKeyspaceManager extends KeyspaceManager {
        public ClusterKeyspaceManager(Cluster cluster) {
            super(cluster, 0);
        }
    }
}
