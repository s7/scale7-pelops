package org.scale7.cassandra.pelops;

import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.thrift.Cassandra.Client;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Management operations need to be applied to a single node.  As a result instances of this class hold a connection so
 * it's very important to call the {@link #release()} method in a finally block.
 *
 * See http://wiki.apache.org/cassandra/LiveSchemaUpdates for more details.
 */
public class KeyspaceManager extends ManagerOperand {
    public static final String KSDEF_STRATEGY_RACK_UNAWARE = "org.apache.cassandra.locator.RackUnawareStrategy";
    public static final String KSDEF_STRATEGY_RACK_AWARE = "org.apache.cassandra.locator.RackAwareStrategy";

    protected KeyspaceManager(Cluster cluster) {
        super(cluster);
    }

    public Set<String> getKeyspaceNames() throws Exception {
        IManagerOperation<Set<String>> operation = new IManagerOperation<Set<String>>() {
            @Override
            public Set<String> execute(Client conn) throws Exception {
                return conn.describe_keyspaces();
            }
        };
        return tryOperation(operation);
    }

    public List<TokenRange> getKeyspaceRingMappings(final String keyspace) throws Exception {
        IManagerOperation<List<TokenRange>> operation = new IManagerOperation<List<TokenRange>>() {
            @Override
            public List<TokenRange> execute(Client conn) throws Exception {
                return conn.describe_ring(keyspace);
            }
        };
        return tryOperation(operation);
    }

    public Map<String, Map<String, String>> getKeyspaceSchema(final String keyspace) throws Exception {
		IManagerOperation<Map<String, Map<String, String>>> operation = new IManagerOperation<Map<String, Map<String, String>>>() {
			@Override
			public Map<String, Map<String, String>> execute(Client conn) throws Exception {
				return conn.describe_keyspace(keyspace);
			}
		};
		return tryOperation(operation);
	}

    public String addKeyspace(final KsDef keyspaceDefinition) throws Exception {
        IManagerOperation<String> operation = new IManagerOperation<String>() {
            @Override
            public String execute(Client conn) throws Exception {
                return conn.system_add_keyspace(keyspaceDefinition);
            }
        };
        return tryOperation(operation);
    }

    public String dropKeyspace(final String keyspace) throws Exception {
        IManagerOperation<String> operation = new IManagerOperation<String>() {
            @Override
            public String execute(Client conn) throws Exception {
                return conn.system_drop_keyspace(keyspace);
            }
        };
        return tryOperation(operation);
    }

    public String renameKeyspace(final String oldName, final String newName) throws Exception {
        IManagerOperation<String> operation = new IManagerOperation<String>() {
            @Override
            public String execute(Client conn) throws Exception {
                return conn.system_rename_keyspace(oldName, newName);
            }
        };
        return tryOperation(operation);
    }
}
