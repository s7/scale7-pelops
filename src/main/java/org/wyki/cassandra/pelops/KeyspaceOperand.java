package org.wyki.cassandra.pelops;

public class KeyspaceOperand extends Operand {

	protected final String keyspace;
	
	protected KeyspaceOperand(ThriftPool thrift, String keyspace) {
		super(thrift);
		this.keyspace = keyspace;
	}

    @Override
    protected ThriftPool.Connection acquireConnection(String lastNode) throws Exception {
        ThriftPool.Connection connection = super.acquireConnection(lastNode);
        connection.getAPI().set_keyspace(keyspace);
        return connection;
    }

    @Override
    protected void releaseConnection(ThriftPool.Connection conn, boolean afterException) {
        try {
            conn.getAPI().set_keyspace(null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        super.releaseConnection(conn, afterException);
    }
}
