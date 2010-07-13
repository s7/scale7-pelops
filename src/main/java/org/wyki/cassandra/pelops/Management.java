package org.wyki.cassandra.pelops;

import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.TokenRange;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Management operations need to be applied to a single node.  As a result instances of this class hold a connection so
 * it's very important to call the {@link #release()} method in a finally block.
 *
 * See http://wiki.apache.org/cassandra/LiveSchemaUpdates for more details.
 */
public class Management extends SingleConnectionOperand {
    protected Management(ThriftPool thrift) {
        super(thrift);
    }

    public Set<String> getKeyspaceNames() throws Exception {
        IOperation<Set<String>> operation = new IOperation<Set<String>>() {
            @Override
            public Set<String> execute(ThriftPool.Connection conn) throws Exception {
                return conn.getAPI().describe_keyspaces();
            }
        };
        return tryOperation(operation);
    }

    public String getClusterName() throws Exception {
        IOperation<String> operation = new IOperation<String>() {
            @Override
            public String execute(ThriftPool.Connection conn) throws Exception {
                return conn.getAPI().describe_cluster_name();
            }
        };
        return tryOperation(operation);
    }

    public String getCassandraVersion() throws Exception {
        IOperation<String> operation = new IOperation<String>() {
            @Override
            public String execute(ThriftPool.Connection conn) throws Exception {
                return conn.getAPI().describe_version();
            }
        };
        return tryOperation(operation);
    }

    public List<TokenRange> getKeyspaceRingMappings(final String keyspace) throws Exception {
        IOperation<List<TokenRange>> operation = new IOperation<List<TokenRange>>() {
            @Override
            public List<TokenRange> execute(ThriftPool.Connection conn) throws Exception {
                return conn.getAPI().describe_ring(keyspace);
            }
        };
        return tryOperation(operation);
    }

    public Map<String, Map<String, String>> getKeyspaceSchema(final String keyspace) throws Exception {
		IOperation<Map<String, Map<String, String>>> operation = new IOperation<Map<String, Map<String, String>>>() {
			@Override
			public Map<String, Map<String, String>> execute(ThriftPool.Connection conn) throws Exception {
				return conn.getAPI().describe_keyspace(keyspace);
			}
		};
		return tryOperation(operation);
	}

    public String addKeyspace(final KsDef keyspaceDef) throws Exception {
        IOperation<String> operation = new IOperation<String>() {
            @Override
            public String execute(ThriftPool.Connection conn) throws Exception {
                return conn.getAPI().system_add_keyspace(keyspaceDef);
            }
        };
        return tryOperation(operation);
    }

    public String dropKeyspace(final String keyspace) throws Exception {
        IOperation<String> operation = new IOperation<String>() {
            @Override
            public String execute(ThriftPool.Connection conn) throws Exception {
                return conn.getAPI().system_drop_keyspace(keyspace);
            }
        };
        return tryOperation(operation);
    }

    public String renameKeyspace(final String oldName, final String newName) throws Exception {
        IOperation<String> operation = new IOperation<String>() {
            @Override
            public String execute(ThriftPool.Connection conn) throws Exception {
                return conn.getAPI().system_rename_keyspace(oldName, newName);
            }
        };
        return tryOperation(operation);
    }
}
