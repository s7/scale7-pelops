package org.wyki.cassandra.pelops;

import org.apache.cassandra.thrift.CfDef;

public class KeyspaceManagement extends SingleConnectionOperand {

    public KeyspaceManagement(ThriftPool thrift) {
        super(thrift);
    }

    public void truncate(final String columnFamily) throws Exception {
        IOperation<Void> operation = new IOperation<Void>() {
            @Override
            public Void execute(ThriftPool.Connection conn) throws Exception {
                conn.getAPI().truncate(columnFamily);
                return null;
            }
        };
        tryOperation(operation);
    }

    public String addColumnFamily(final CfDef columnFamilyDef) throws Exception {
        IOperation<String> operation = new IOperation<String>() {
            @Override
            public String execute(ThriftPool.Connection conn) throws Exception {
                return conn.getAPI().system_add_column_family(columnFamilyDef);
            }
        };
        return tryOperation(operation);
    }

    public String dropColumnFamily(final String columnFamily) throws Exception {
        IOperation<String> operation = new IOperation<String>() {
            @Override
            public String execute(ThriftPool.Connection conn) throws Exception {
                return conn.getAPI().system_drop_column_family(columnFamily);
            }
        };
        return tryOperation(operation);
    }

    public String renameColumnFamily(final String oldName, final String newName) throws Exception {
        IOperation<String> operation = new IOperation<String>() {
            @Override
            public String execute(ThriftPool.Connection conn) throws Exception {
                return conn.getAPI().system_rename_column_family(oldName, newName);
            }
        };
        return tryOperation(operation);
    }
}