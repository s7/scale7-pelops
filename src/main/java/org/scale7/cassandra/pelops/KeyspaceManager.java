package org.scale7.cassandra.pelops;

import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

import java.util.List;

/**
 * Management operations need to be applied to a single node.
 *
 * See http://wiki.apache.org/cassandra/LiveSchemaUpdates for more details.
 */
public class KeyspaceManager extends ManagerOperand {
    private static final Logger logger = SystemProxy.getLoggerFromFactory(KeyspaceManager.class);

    public static final String KSDEF_STRATEGY_SIMPLE = "org.apache.cassandra.locator.SimpleStrategy";
    public static final String KSDEF_STRATEGY_LOCAL = "org.apache.cassandra.locator.LocalStrategy";
    public static final String KSDEF_STRATEGY_NETWORK_TOPOLOGY = "org.apache.cassandra.locator.NetworkTopologyStrategy";
    public static final String KSDEF_STRATEGY_NETWORK_TOPOLOGY_OLD = "org.apache.cassandra.locator.OldNetworkTopologyStrategy";

    public KeyspaceManager(Cluster cluster) {
        super(cluster);
    }

    public KeyspaceManager(Cluster cluster, int safeNodeChangeDelay) {
        super(cluster, safeNodeChangeDelay);
    }

    public List<KsDef> getKeyspaceNames() throws Exception {
        IManagerOperation<List<KsDef>> operation = new IManagerOperation<List<KsDef>>() {
            @Override
            public List<KsDef> execute(Client conn) throws Exception {
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

    public KsDef getKeyspaceSchema(final String keyspace) throws Exception {
		IManagerOperation<KsDef> operation = new IManagerOperation<KsDef>() {
			@Override
			public KsDef execute(Client conn) throws Exception {
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

    /* - https://issues.apache.org/jira/browse/CASSANDRA-1630
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
    */
}
