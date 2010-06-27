package org.wyki.cassandra.pelops;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.thrift.TokenRange;
import org.wyki.cassandra.pelops.ThriftPool.Connection;

/**
 * Retrieves information about the Cassandra cluster the keyspaces it contains.
 * 
 * @author dominicwilliams
 *
 */
public class Metrics extends Operand {
	
	/**
	 * Get the names of the keyspaces maintained by this cluster
	 * @return							The keyspace names
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public Set<String> getKeyspaceNames() throws Exception {
		IOperation operation = new IOperation() {
			@Override
			public Object execute(Connection conn) throws Exception {
				return conn.getAPI().describe_keyspaces();
			}
		};
		return (Set<String>) tryOperation(operation);
	}
	
	/**
	 * Get the name of this Cassandra cluster.
	 * @return							The cluster name		
	 * @throws Exception
	 */
	public String getClusterName() throws Exception {
		IOperation operation = new IOperation() {
			@Override
			public Object execute(Connection conn) throws Exception {
				return conn.getAPI().describe_cluster_name();
			}
		};
		return (String) tryOperation(operation);
	}
	
	/**
	 * Get the version of the Cassandra software this cluster is running on.
	 * @return							A description of the software version
	 * @throws Exception
	 */
	public String getCassandraVersion() throws Exception {
		IOperation operation = new IOperation() {
			@Override
			public Object execute(Connection conn) throws Exception {
				return conn.getAPI().describe_version();
			}
		};
		return (String) tryOperation(operation);
	}
	
	/**
	 * Get the mapping of keys to end points (cluster nodes) for this keyspace
	 * @param keyspace					The keyspace name
	 * @return							A list of the mappings from ranges to end points
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public List<TokenRange> getKeyspaceRingMappings(final String keyspace) throws Exception {
		IOperation operation = new IOperation() {
			@Override
			public Object execute(Connection conn) throws Exception {
				return conn.getAPI().describe_ring(keyspace);
			}
		};
		return (List<TokenRange>) tryOperation(operation);
	}
	
	/**
	 * Get the scheme of a keyspace
	 * @param keyspace					The keyspace name
	 * @return							The schema of the keyspace
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Map<String, String>> getKeyspaceSchema(final String keyspace) throws Exception {
		IOperation operation = new IOperation() {
			@Override
			public Object execute(Connection conn) throws Exception {
				return conn.getAPI().describe_keyspace(keyspace);
			}
		};
		return (Map<String, Map<String, String>>) tryOperation(operation);
	}
	
	protected Metrics(ThriftPool thrift) {
		super(thrift);
	}

}
