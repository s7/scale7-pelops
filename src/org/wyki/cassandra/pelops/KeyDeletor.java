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
	 * Delete all rows with the specified key (that is, for each column family, remove any row that has the
	 * specified key, such that the database no longer stores any data for the key).
	 * @param rowKey					The key of the rows to be deleted
	 * @param cLevel					The Cassandra consistency level to be used
	 * @throws Exception
	public void deleteKey(final String rowKey, final ConsistencyLevel cLevel) throws Exception {
		IOperation operation = new IOperation() {
			@Override
			public Object execute(Connection conn) throws Exception {

				ColumnPath path = new ColumnPath();
				conn.getAPI().remove(keyspace, rowKey, path, timestamp, cLevel);
				return null;
			}
		};
		tryOperation(operation);
		throw new NotImplementedException();
	}
	*/

	/**
	 * Delete a row with a specified key from a specified column family. The function succeeds even if
	 * the row does not exist.
	 * @param rowKey					The key of the row
	 * @param columnFamily				The column family from which to delete the row
	 * @param cLevel					The Cassandra consistency level to be used
	 * @throws Exception
	 */
	public void deleteRow(final String rowKey, final String columnFamily, final ConsistencyLevel cLevel) throws Exception {
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
