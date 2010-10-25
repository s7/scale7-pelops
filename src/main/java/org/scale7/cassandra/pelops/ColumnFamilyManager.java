package org.scale7.cassandra.pelops;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Cassandra.Client;

public class ColumnFamilyManager extends ManagerOperand {

    public static final String CFDEF_TYPE_STANDARD = "Standard";
    public static final String CFDEF_TYPE_SUPER = "Super";

    public static final String CFDEF_COMPARATOR_BYTES = "BytesType";
    public static final String CFDEF_COMPARATOR_ASCII = "AsciiType";
    public static final String CFDEF_COMPARATOR_UTF8 = "UTF8Type";
    public static final String CFDEF_COMPARATOR_LONG = "LongType";
    public static final String CFDEF_COMPARATOR_LEXICAL_UUID = "LexicalUUIDType";
    public static final String CFDEF_COMPARATOR_TIME_UUID = "TimeUUIDType";
    public static final String CFDEF_COMPARATOR_INTEGER = "IntegerType";

    public ColumnFamilyManager(Cluster cluster, String keyspace) {
        super(cluster, keyspace);
    }

    public void truncateColumnFamily(final String columnFamily) throws Exception {
    	IManagerOperation<Void> operation = new IManagerOperation<Void>() {
            @Override
            public Void execute(Client conn) throws Exception {
                conn.truncate(columnFamily);
                return null;
            }
        };
        tryOperation(operation);
    }

    public String addColumnFamily(final CfDef columnFamilyDefinition) throws Exception {
    	IManagerOperation<String> operation = new IManagerOperation<String>() {
            @Override
            public String execute(Client conn) throws Exception {
                return conn.system_add_column_family(columnFamilyDefinition);
            }
        };
        return tryOperation(operation);
    }
    
    /**
     * Update the column family 
     * @param columnFamilyDefinition
     * @return
     * @throws Exception
     */
    public String updateColumnFamily(final CfDef columnFamilyDefinition) throws Exception {
    	IManagerOperation<String> operation = new IManagerOperation<String>() {
            @Override
            public String execute(Client conn) throws Exception {
                return conn.system_update_column_family(columnFamilyDefinition);
            }
        };
        return tryOperation(operation);
    }

    public String dropColumnFamily(final String columnFamily) throws Exception {
    	IManagerOperation<String> operation = new IManagerOperation<String>() {
            @Override
            public String execute(Client conn) throws Exception {
                return conn.system_drop_column_family(columnFamily);
            }
        };
        return tryOperation(operation);
    }
    

   


    /* - https://issues.apache.org/jira/browse/CASSANDRA-1630
    public String renameColumnFamily(final String oldName, final String newName) throws Exception {
    	IManagerOperation<String> operation = new IManagerOperation<String>() {
            @Override
            public String execute(Client conn) throws Exception {
                return conn.system_rename_column_family(oldName, newName);
            }
        };
        return tryOperation(operation);
    }
    */
}