package org.scale7.cassandra.pelops.support;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.junit.Before;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.ColumnFamilyManager;
import org.scale7.cassandra.pelops.pool.CommonsBackedPool;
import org.scale7.cassandra.pelops.pool.DebuggingPool;
import org.scale7.cassandra.pelops.pool.IThriftPool;
import org.scale7.cassandra.pelops.KeyspaceManager;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.OperandPolicy;
import org.scale7.cassandra.pelops.Selector;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

/**
 * Abstract integration test class which runs its own embedded cassandra server.
 */
public abstract class AbstractIntegrationTest {

	protected final Logger logger = SystemProxy.getLoggerFromFactory(this
			.getClass());

	public static final String RPC_LISTEN_ADDRESS = "localhost";

	public static final int RPC_PORT = 19160;
	
	public static String BASE_DIRECTORY = "target/cassandra";

	public static final String KEYSPACE = "PelopsTesting";

	private static EmbeddedCassandraServer cassandraServer;

	public static Cluster cluster = new Cluster(RPC_LISTEN_ADDRESS, RPC_PORT);

	private static KeyspaceManager keyspaceManager;

	private static ColumnFamilyManager columnFamilyManager;

	private static List<CfDef> colFamilyDefs;

	private IThriftPool pool = new DebuggingPool(cluster, KEYSPACE, new OperandPolicy());

	public KeyspaceManager getKeyspaceManager() {
		return keyspaceManager;
	}

	public ColumnFamilyManager getColumnFamilyManager() {
		return columnFamilyManager;
	}

	protected Cluster getCluster() {
		return cluster;
	}

	protected IThriftPool getPool() {
		return pool;
	}

	/**
	 * Starts embedded cassandra server.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	public static void setup(List<CfDef> columnDefinitions) throws Exception {
		if (cassandraServer == null) {
			cassandraServer = new EmbeddedCassandraServer(RPC_LISTEN_ADDRESS,
					RPC_PORT, BASE_DIRECTORY);
			cassandraServer.start();

			// wait until cassandra server starts up. could wait less time, but
			// 2 seconds to be sure.
			Thread.sleep(2000);
		}

		colFamilyDefs = columnDefinitions;

		keyspaceManager = new KeyspaceManager(cluster);
		columnFamilyManager = new ColumnFamilyManager(cluster, KEYSPACE);

		List<KsDef> keyspaces = keyspaceManager.getKeyspaceNames();
		for (KsDef ksDef : keyspaces)
			if (ksDef.name.equals(KEYSPACE)) {
				keyspaceManager.dropKeyspace(KEYSPACE);
			}

		KsDef keyspaceDefinition = new KsDef(KEYSPACE,
				KeyspaceManager.KSDEF_STRATEGY_SIMPLE, 1,
				new ArrayList<CfDef>());

		for (CfDef colFamilyDef : colFamilyDefs) {
			keyspaceDefinition.addToCf_defs(colFamilyDef);
		}

		keyspaceManager.addKeyspace(keyspaceDefinition);
	}

	/**
	 * Database prepare before test.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Before
	public void setupTest() throws Exception {
		truncate();
		prepareData();
	}

	/**
	 * Data prepare. Its purpose is to be overloaded from test class.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	public void prepareData() throws Exception {
	}

	/**
	 * Truncate all column families.
	 */
	public void truncate() {
		for (CfDef colFamilyDef : colFamilyDefs) {
			try {
				columnFamilyManager
						.truncateColumnFamily(colFamilyDef.getName());
			} catch (Exception e) {
				throw new IllegalStateException(
						"Failed to truncate column family "
								+ colFamilyDef.getName(), e);
			}
		}
	}

	/**
	 * @return new mutator instance
	 */
	public Mutator createMutator() {
		return pool.createMutator();
	}

	/**
	 * @return new selector instance
	 */
	public Selector createSelector() {
		return pool.createSelector();
	}
}
