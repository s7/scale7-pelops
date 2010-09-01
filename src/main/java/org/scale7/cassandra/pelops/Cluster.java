package org.scale7.cassandra.pelops;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.thrift.TokenRange;

public class Cluster {
	final int thriftPort;
	final String[] contactNodes;
	String[] currentNodes;

    public Cluster(String contactNodes, int thriftPort) {
        this(splitAndTrim(contactNodes), thriftPort);
    }

    public Cluster(String[] contactNodes, int thriftPort) {
		this.thriftPort = thriftPort;
		this.contactNodes = contactNodes;
		this.currentNodes = contactNodes;
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
	 * The thrift port on which the cluster listens
	 * @return
	 */
	public int getThriftPort() {
		return thriftPort;
	}

	/**
	 * Get a snapshot of the list of nodes currently believed to exist in the Cassandra cluster. You must
	 * iterate snapshot that is returned, rather than iterating the property directly, since the list may
	 * change at any time.
	 * @return			A snapshot of the nodes in the cluster, as are currently believed to exist
	 */
	public String[] getCurrentNodesSnapshot() {
		return currentNodes;
	}

	/**
	 * Refresh the snapshot of the list of nodes currently believed to exist in the Cassandra cluster.
	 * @throws Exception
	 */
	public void refreshNodesSnapshot() throws Exception {
		KeyspaceManager kspcMngr = Pelops.createKeyspaceManager(this);
		Set<String> keyspaces = kspcMngr.getKeyspaceNames();
		Iterator<String> k = keyspaces.iterator();
		String appKeyspace = null;
		while (k.hasNext()) {
			String keyspace = k.next();
			if (!keyspace.equals("system")) {
				appKeyspace = keyspace;
				break;
			}
		}
		if (appKeyspace == null)
			throw new Exception("Cannot obtain a node list from a ring mapping. No keyspaces are defined for this cluster.");
		List<TokenRange> mappings = kspcMngr.getKeyspaceRingMappings(appKeyspace);
		HashSet<String> clusterNodes = new HashSet<String>();
		for (TokenRange tokenRange : mappings) {
			List<String> endPointList = tokenRange.getEndpoints();
			clusterNodes.addAll(endPointList);
		}
		currentNodes = clusterNodes.toArray(new String[] {});
	}


    private boolean isFramedTransportRequired = true;

    /**
     * Used to determine if the thrift transport should be framed or not.  This is dicated by the 'thrift_framed_transport_size_in_mb'
     * property in cassandra.yaml.
     * @return true if framed transport should be used, otherwise false.
     */
    public boolean isFramedTransportRequired() {
        return isFramedTransportRequired;
    }

    /**
     * Used to determine if the thrift transport should be framed or not.  This is dicated by the 'thrift_framed_transport_size_in_mb'
     * property in cassandra.yaml.
     * @param  framedTransportRequired true if framed transport should be used, otherwise false.
     */
    public void setFramedTransportRequired(boolean framedTransportRequired) {
        isFramedTransportRequired = framedTransportRequired;
    }
}
