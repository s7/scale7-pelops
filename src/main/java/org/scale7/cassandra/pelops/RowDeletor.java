package org.scale7.cassandra.pelops;

import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.scale7.cassandra.pelops.exceptions.PelopsException;
import org.scale7.cassandra.pelops.pool.IThriftPool.IPooledConnection;
import org.scale7.cassandra.pelops.pool.IThriftPool;

import static org.scale7.cassandra.pelops.Bytes.fromUTF8;
import static org.scale7.cassandra.pelops.Bytes.nullSafeGet;

/**
 * Facilitates the removal of data at a key-level.
 *
 * @author dominicwilliams
 *
 */
public class RowDeletor extends Operand {

	private final long timestamp;

	/**
	 * Delete a row with a specified key from a specified column family. The function succeeds even if
	 * the row does not exist.
	 * @param columnFamily				The column family from which to delete the row
	 * @param rowKey					The key of the row
	 * @param cLevel					The Cassandra consistency level to be used
	 * @throws PelopsException
	 */
	public void deleteRow(final String columnFamily, final String rowKey, final ConsistencyLevel cLevel) throws PelopsException {
		deleteRow(columnFamily, fromUTF8(rowKey), cLevel);
	}

	/**
	 * Delete a row with a specified key from a specified column family. The function succeeds even if
	 * the row does not exist.
	 * @param columnFamily				The column family from which to delete the row
	 * @param rowKey					The key of the row
	 * @param cLevel					The Cassandra consistency level to be used
	 * @throws PelopsException
	 */
	public void deleteRow(final String columnFamily, final Bytes rowKey, final ConsistencyLevel cLevel) throws PelopsException {
		IOperation<Void> operation = new IOperation<Void>() {
			@Override
			public Void execute(IPooledConnection conn) throws Exception {

				ColumnPath path = new ColumnPath(columnFamily);
				conn.getAPI().remove(nullSafeGet(rowKey), path, timestamp, cLevel);
				return null;
			}
		};
		tryOperation(operation);
	}

	public RowDeletor(IThriftPool thrift) {
		this(thrift, System.currentTimeMillis() * 1000);
	}

	public RowDeletor(IThriftPool thrift, long timestamp) {
		super(thrift);
        this.timestamp = timestamp;
	}
}
