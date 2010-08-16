package org.scale7.cassandra.pelops;

import org.apache.cassandra.thrift.Cassandra.Client;

public class ClusterManager extends ManagerOperand {

	ClusterManager(Cluster cluster) {
		super(cluster, null, 0);
	}

	/**
	 * Get the name of the cluster. Only nodes with the same cluster name communicate using the Gossip P2P protocol.
	 * @return					The name of the cluster
	 * @throws Exception
	 */
    public String getClusterName() throws Exception {
        IManagerOperation<String> operation = new IManagerOperation<String>() {
            @Override
            public String execute(Client conn) throws Exception {
                return conn.describe_cluster_name();
            }
        };
        return tryOperation(operation);
    }

    /**
     * Get the version of the Cassandra software being run by the cluster.
     * @return					The version of the Cassandra software
     * @throws Exception
     */
    public String getCassandraVersion() throws Exception {
        IManagerOperation<String> operation = new IManagerOperation<String>() {
            @Override
            public String execute(Client conn) throws Exception {
                return conn.describe_version();
            }
        };
        return tryOperation(operation);
    }

}
