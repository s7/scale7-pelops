package org.scale7.cassandra.pelops;

import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Management operations need to be applied to a single node.
 *
 * See http://wiki.apache.org/cassandra/LiveSchemaUpdates for more details.
 */
public class KeyspaceManager extends ManagerOperand {
    private static final Logger logger = SystemProxy.getLoggerFromFactory(KeyspaceManager.class);

    public static final String KSDEF_STRATEGY_RACK_UNAWARE = "org.apache.cassandra.locator.RackUnawareStrategy";
    public static final String KSDEF_STRATEGY_RACK_AWARE = "org.apache.cassandra.locator.RackAwareStrategy";

    public KeyspaceManager(Cluster cluster) {
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
        if (logger.isInfoEnabled()) logger.info("Adding keyspace '{}'", keyspaceDefinition.getName());
        IManagerOperation<String> operation = new IManagerOperation<String>() {
            @Override
            public String execute(Client conn) throws Exception {
                return conn.system_add_keyspace(keyspaceDefinition);
            }
        };
        String schemaVersion = tryOperation(operation);
        if (logger.isInfoEnabled()) logger.info("Added keyspace '{}', schema version is now '{}'", new Object[] {keyspaceDefinition.getName(), schemaVersion});

        return schemaVersion;
    }

    public String dropKeyspace(final String keyspace) throws Exception {
        if (logger.isInfoEnabled()) logger.info("Dropping keyspace '{}'", keyspace);
        IManagerOperation<String> operation = new IManagerOperation<String>() {
            @Override
            public String execute(Client conn) throws Exception {
                return conn.system_drop_keyspace(keyspace);
            }
        };
        String schemaVersion = tryOperation(operation);
        if (logger.isInfoEnabled()) logger.info("Dropped keyspace '{}', schema version is now '{}'", keyspace, schemaVersion);

        return schemaVersion;
    }

    public String renameKeyspace(final String oldName, final String newName) throws Exception {
        if (logger.isInfoEnabled()) logger.info("Renaming keyspace '{}' to '{}'", oldName, newName);
        IManagerOperation<String> operation = new IManagerOperation<String>() {
            @Override
            public String execute(Client conn) throws Exception {
                return conn.system_rename_keyspace(oldName, newName);
            }
        };
        String schemaVersion = tryOperation(operation);
        if (logger.isInfoEnabled()) logger.info("Renamed keyspace '{}' to '{}', schema version is now '{}'", new Object[] {oldName, newName, schemaVersion});

        return schemaVersion;
    }
}
