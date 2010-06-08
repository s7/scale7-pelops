package org.wyki.cassandra.pelops;

import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.wyki.cassandra.pelops.ThriftPool.Connection;

/**
 * Facilitates the removal of data at a key-level. 
 * 
 * @author dominicwilliams
 *
 */
public class KeyDeletor extends KeyspaceOperand {

	private final long timestamp;
	
	/**
	 * Delete an entire row, which is identified by its key, and all of the column families that it contains.
	 * @param rowKey					The key of the row to delete
	 * @param cLevel					The Cassandra consistency level to be used
	 * @throws Exception
	 */
	public void deleteRow(final String rowKey, final ConsistencyLevel cLevel) throws Exception {
		IOperation operation = new IOperation() {
			@Override
			public Object execute(Connection conn) throws Exception {
				
				ColumnPath path = new ColumnPath();
				conn.getAPI().remove(keyspace, rowKey, path, timestamp, cLevel);	
				return null;
			}
		};
		tryOperation(operation);
	}
	
	/**
	 * Delete an entire column family from a row, which is identified by its key.
	 * @param rowKey					The key of the row
	 * @param columnFamily				The column family to delete
	 * @param cLevel					The Cassandra consistency level to be used
	 * @throws Exception
	 */
	public void deleteColumnFamily(final String rowKey, final String columnFamily, final ConsistencyLevel cLevel) throws Exception {
		IOperation operation = new IOperation() {
			@Override
			public Object execute(Connection conn) throws Exception {
				
				ColumnPath path = new ColumnPath(columnFamily);
				conn.getAPI().remove(keyspace, rowKey, path, timestamp, cLevel);
				return null;
			}
		};
		tryOperation(operation);
	}
	
	protected KeyDeletor(ThriftPool thrift, String keyspace) {
		this(thrift, keyspace, System.currentTimeMillis() * 1000);
	}
	
	protected KeyDeletor(ThriftPool thrift, String keyspace, long timestamp) {
		super(thrift, keyspace);
		this.timestamp = timestamp;
	}
}
