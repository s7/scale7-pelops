package org.scale7.cassandra.pelops;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
* Created by IntelliJ IDEA.
* User: dan
* Date: Sep 14, 2010
* Time: 12:10:56 PM
* To change this template use File | Settings | File Templates.
*/
public class IntegrationTestHelper {
    private static final Logger logger = SystemProxy.getLoggerFromFactory(IntegrationTestHelper.class);

    public static final String KEYSPACE = "PelopsTesting";

    private Cluster cluster = new Cluster("localhost", 9160);
    private KeyspaceManager keyspaceManager;
    private ColumnFamilyManager columnFamilyManager;
    private IThriftPool pool;
    private List<CfDef> colFamilyDefs;

    public void setup(List<CfDef> colFamilyDefs) throws Exception {
        this.colFamilyDefs = colFamilyDefs;
        keyspaceManager = new KeyspaceManager(cluster);
        columnFamilyManager = new ColumnFamilyManager(cluster, KEYSPACE);

        // start from scratch
        List<KsDef> keyspaces = keyspaceManager.getKeyspaceNames();
        for (KsDef ksDef : keyspaces)
            if (ksDef.name.equals(KEYSPACE)) {
                logger.info("Dropping keyspace '{}'", KEYSPACE);
                keyspaceManager.dropKeyspace(KEYSPACE);
            }

        KsDef keyspaceDefinition = new KsDef(KEYSPACE, KeyspaceManager.KSDEF_STRATEGY_SIMPLE, 1, new ArrayList<CfDef>());

        for (CfDef colFamilyDef : colFamilyDefs) {
            logger.info("Adding column family '{}'", colFamilyDef.getName());
            keyspaceDefinition.addToCf_defs(
                colFamilyDef
            );
        }

        keyspaceManager.addKeyspace(keyspaceDefinition);

        pool = new DebuggingPool(cluster, KEYSPACE, new OperandPolicy());
    }

    public void truncate() {
        for (CfDef colFamilyDef : colFamilyDefs) {
            try {
                columnFamilyManager.truncateColumnFamily(colFamilyDef.getName());
            } catch (Exception e) {
                throw new IllegalStateException("Failed to truncate column family " + colFamilyDef.getName(), e);
            }
        }
    }

    public void teardown() {
        if (pool != null)
            pool.shutdown();
    }

    public Cluster getCluster() {
        return cluster;
    }

    protected IThriftPool getPool() {
        return pool;
    }
}
