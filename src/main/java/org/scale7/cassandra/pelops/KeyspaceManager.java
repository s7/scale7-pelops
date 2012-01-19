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

import java.util.List;

import org.apache.cassandra.thrift.Cassandra.AsyncClient;
import org.apache.cassandra.thrift.Cassandra.AsyncClient.describe_keyspace_call;
import org.apache.cassandra.thrift.Cassandra.AsyncClient.describe_keyspaces_call;
import org.apache.cassandra.thrift.Cassandra.AsyncClient.describe_ring_call;
import org.apache.cassandra.thrift.Cassandra.AsyncClient.system_add_keyspace_call;
import org.apache.cassandra.thrift.Cassandra.AsyncClient.system_drop_keyspace_call;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.thrift.async.AsyncMethodCallback;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

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
        IManagerOperation<describe_keyspaces_call, List<KsDef>> operation = new IManagerOperation<describe_keyspaces_call, List<KsDef>>() {

            @Override
            public void execute(AsyncClient conn, AsyncMethodCallback<describe_keyspaces_call> callback)
                    throws Exception {
                conn.describe_keyspaces(callback);
            }

            @Override
            public List<KsDef> getResult(describe_keyspaces_call call)
                    throws Exception {
                return call.getResult();
            }
        };
        return tryOperation(operation);
    }

    public List<TokenRange> getKeyspaceRingMappings(final String keyspace) throws Exception {
        IManagerOperation<describe_ring_call, List<TokenRange>> operation = new IManagerOperation<describe_ring_call, List<TokenRange>>() {

            @Override
            public void execute(AsyncClient conn, AsyncMethodCallback<describe_ring_call> callback)
                    throws Exception {
                conn.describe_ring(keyspace, callback);
            }

            @Override
            public List<TokenRange> getResult(describe_ring_call call)
                    throws Exception {
                return call.getResult();
            }
        };
        return tryOperation(operation);
    }

    public KsDef getKeyspaceSchema(final String keyspace) throws Exception {
		IManagerOperation<describe_keyspace_call, KsDef> operation = new IManagerOperation<describe_keyspace_call, KsDef>() {
			@Override
			public void execute(AsyncClient conn, AsyncMethodCallback<describe_keyspace_call> callback) throws Exception {
				conn.describe_keyspace(keyspace, callback);
			}

            @Override
            public KsDef getResult(describe_keyspace_call call) throws Exception {
                return call.getResult();
            }
		};
		return tryOperation(operation);
	}

    public String addKeyspace(final KsDef keyspaceDefinition) throws Exception {
        if (logger.isInfoEnabled()) logger.info("Adding keyspace '{}'", keyspaceDefinition.getName());
        IManagerOperation<system_add_keyspace_call, String> operation = new IManagerOperation<system_add_keyspace_call, String>() {

            @Override
            public void execute(AsyncClient conn, AsyncMethodCallback<system_add_keyspace_call> callback)
                    throws Exception {
                conn.system_add_keyspace(keyspaceDefinition, callback);
            }

            @Override
            public String getResult(system_add_keyspace_call call)
                    throws Exception {
                return call.getResult();
            }
        };
        String schemaVersion = tryOperation(operation);
        if (logger.isInfoEnabled()) logger.info("Added keyspace '{}', schema version is now '{}'", new Object[] {keyspaceDefinition.getName(), schemaVersion});

        return schemaVersion;
    }

    public String dropKeyspace(final String keyspace) throws Exception {
        if (logger.isInfoEnabled()) logger.info("Dropping keyspace '{}'", keyspace);
        IManagerOperation<system_drop_keyspace_call, String> operation = new IManagerOperation<system_drop_keyspace_call, String>() {

            @Override
            public void execute(AsyncClient conn,
                    AsyncMethodCallback<system_drop_keyspace_call> callback)
                    throws Exception {
                conn.system_drop_keyspace(keyspace, callback);
            }

            @Override
            public String getResult(system_drop_keyspace_call call)
                    throws Exception {
                return call.getResult();
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
