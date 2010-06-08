package org.wyki.cassandra.pelops;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SuperColumn;
import org.wyki.cassandra.pelops.ThriftPool.Connection;
import static org.wyki.cassandra.pelops.StringHelper.toBytes;

/**
 * Facilitates the mutation of data within a Cassandra keyspace: the desired mutations should first be specified by
 * calling methods such as <code>writeColumn(...)</code>, which should then be sent to Cassandra in a single batch by
 * calling <code>execute(...)</code>. After the desired batch of mutations has been executed, the <code>Mutator</code>
 * object can not be re-used.
 * 
 * @author dominicwilliams
 *
 */
public class Mutator extends KeyspaceOperand {
	
	/**
	 * Execute the mutations that have been specified by sending them to Cassandra in a single batch.
	 * @param cLevel					The Cassandra consistency level to be used
	 * @throws Exception				
	 */
	public void execute(final ConsistencyLevel cLevel) throws Exception {
		IOperation operation = new IOperation() {
			@Override
			public Object execute(Connection conn) throws Exception {
				// Send batch mutation job to Thrift connection
				conn.getAPI().batch_mutate(keyspace, batch, cLevel);
				// Flush connection
				conn.flush();
				// Nothing to return
				return null;
			}
		};
		tryOperation(operation);
	}
		
	/**
	 * Write a column value.
	 * @param rowKey					The key of the row to modify
	 * @param colFamily					The name of the column family to modify
	 * @param column					The value of the column
	 */
	public void writeColumn(String rowKey, String colFamily, Column column) {
		ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
		cosc.setColumn(column);
		Mutation mutation = new Mutation();
		mutation.setColumn_or_supercolumn(cosc);
		getMutationList(rowKey, colFamily).add(mutation);
	}
	
	/**
	 * Write a list of columns to a key
	 * @param rowKey					The key of the row to modify
	 * @param colFamily					The name of the column family to modify
	 * @param columns					The list of columns to write
	 */
	public void writeColumns(String rowKey, String colFamily, List<Column> columns) {
		for (Column column : columns) {
			writeColumn(rowKey, colFamily, column);
		}
	}
	
	/**
	 * Write a single sub-column value to a super column. If wish to write multiple sub-columns for a
	 * super column, then it is more efficient to use <code>writeSubColumns</code>
	 * @param rowKey					The key of the row to modify
	 * @param colFamily					The name of the super column family to operate on
	 * @param colName					The name of the super column
	 * @param subColumn					The sub-column
	 */
	public void writeSubColumn(String rowKey, String colFamily, String colName, Column subColumn) {
		writeSubColumn(rowKey, colFamily, toBytes(colName), subColumn);
	}
	
	/**
	 * Write a single sub-column value to a super column. If wish to write multiple sub-columns for a
	 * super column, then it is more efficient to use <code>writeSubColumns</code>
	 * @param rowKey					The key of the row to modify
	 * @param colFamily					The name of the super column family to operate on
	 * @param colName					The name of the super column
	 * @param subColumn					The sub-column
	 */
	public void writeSubColumn(String rowKey, String colFamily, byte[] colName, Column subColumn) {
		List<Column> subColumns = new ArrayList<Column>(1);
		subColumns.add(subColumn);
		writeSubColumns(rowKey, colFamily, colName, subColumns);
	}
	
	/**
	 * Write multiple sub-column values to a super column.
	 * @param rowKey					The key of the row to modify
	 * @param colFamily					The name of the super column family to operate on
	 * @param colName					The name of the super column
	 * @param subColumns				A list of the sub-columns to write
	 */
	public void writeSubColumns(String rowKey, String colFamily, String colName, List<Column> subColumns) {
		writeSubColumns(rowKey, colFamily, toBytes(colName), subColumns);
	}
	
	/**
	 * Write multiple sub-column values to a super column.
	 * @param rowKey					The key of the row to modify
	 * @param colFamily					The name of the super column family to operate on
	 * @param colName					The name of the super column
	 * @param subColumns				A list of the sub-columns to write
	 */
	public void writeSubColumns(String rowKey, String colFamily, byte[] colName, List<Column> subColumns) {
		SuperColumn scol = new SuperColumn(colName, subColumns);
		ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
		cosc.setSuper_column(scol);
		Mutation mutation = new Mutation();
		mutation.setColumn_or_supercolumn(cosc);
		getMutationList(rowKey, colFamily).add(mutation);
	}
	
	/**
	 * Delete a column or super column
	 * @param rowKey					The key of the row to modify
	 * @param colFamily					The name of the column family to modify
	 * @param colName					The name of the column or super column to delete.
	 */
	public void deleteColumn(String rowKey, String colFamily, String colName) {
		deleteColumn(rowKey, colFamily, toBytes(colName));
	}
	
	/**
	 * Delete a column or super column.
	 * @param rowKey					The key of the row to modify
	 * @param colFamily					The name of the column family to modify
	 * @param colName					The name of the column or super column to delete.
	 */
	public void deleteColumn(String rowKey, String colFamily, byte[] colName) {
		List<byte[]> colNames = new ArrayList<byte[]>(1);
		colNames.add(colName);
		deleteColumns(rowKey, colFamily, colNames);
	}
	
	/**
	 * Delete a list of columns or super columns.
	 * @param rowKey					The key of the row to modify
	 * @param colFamily					The name of the column family to modify
	 * @param colNames					The column and/or super column names to delete
	 */
	public void deleteColumns(String rowKey, String colFamily, byte[]... colNames) {
		List<byte[]> colNameList = new ArrayList<byte[]>(Arrays.asList(colNames));
		deleteColumns(rowKey, colFamily, colNameList);
	}
		
	/**
	 * Delete a list of columns or super columns.
	 * @param rowKey					The key of the row to modify
	 * @param colFamily					The name of the column family to modify
	 * @param colNames					The column and/or super column names to delete
	 * @throws UnsupportedEncodingException 
	 */
	public void deleteColumns(String rowKey, String colFamily, String... colNames) {
		List<byte[]> colNameList = new ArrayList<byte[]>(colNames.length);
		for (String colName : colNames) 
			colNameList.add(toBytes(colName));
		deleteColumns(rowKey, colFamily, colNameList);
	}
	
	/**
	 * Delete a list of columns or super columns.
	 * @param rowKey					The key of the row to modify
	 * @param colFamily					The name of the column family to modify
	 * @param colNames					The column and/or super column names to delete
	 */
	public void deleteColumns(String rowKey, String colFamily, List<byte[]> colNames) {
		SlicePredicate pred = new SlicePredicate();
		pred.setColumn_names(colNames);
		Deletion deletion = new Deletion(timestamp);
		deletion.setPredicate(pred);
		Mutation mutation = new Mutation();
		mutation.setDeletion(deletion);
		getMutationList(rowKey, colFamily).add(mutation);
	}
	
	/**
	 * Create new Column object with the time stamp passed to the constructor
	 * @param colName					The column name
	 * @param colValue					The column value
	 * @return							An appropriate <code>Column</code> object
	 * @throws UnsupportedEncodingException
	 */
	public Column newColumn(String colName, String colValue) {
		return newColumn(colName, toBytes(colValue));
	}
	
	/**
	 * Create new Column object with the time stamp passed to the constructor
	 * @param colName					The column name
	 * @param colValue					The column value
	 * @return							An appropriate <code>Column</code> object
	 * @throws UnsupportedEncodingException
	 */
	public Column newColumn(byte[] colName, String colValue) {
		return newColumn(colName, toBytes(colValue));
	}
	
	/**
	 * Create new Column object with the time stamp passed to the constructor
	 * @param colName					The column name
	 * @param colValue					The column value
	 * @return							An appropriate <code>Column</code> object
	 * @throws UnsupportedEncodingException
	 */
	public Column newColumn(String colName, byte[] colValue) {
		return newColumn(toBytes(colName), colValue);
	}
	
	/**
	 * Create new Column object with the time stamp passed to the constructor
	 * @param colName					The column name
	 * @param colValue					The column value
	 * @return							An appropriate <code>Column</code> object
	 * @throws UnsupportedEncodingException
	 */
	public Column newColumn(byte[] colName, byte[] colValue) {
		return new Column(colName, colValue, timestamp);
	}
	
	/**
	 * Create a list of <code>Column</code> objects.
	 * @param columns					The columns from which to compose the list
	 * @return							A list of <code>Column</code> objects
	 */
	public List<Column> newColumnList(Column... columns) {
		ArrayList<Column> list = new ArrayList<Column>(columns.length);
		for (Column column : columns)
			list.add(column);
		return list;
	}
	
	/**
	 * Get the default time stamp used by this <code>Mutator</code> instance as a byte[].
	 * @param microsToMillis			If the time stamp is UTC microseconds (as is a self-constructed time stamp), whether to convert this into a standard milliseconds value
	 * @return							A byte array containing the time stamp <code>long</code> value
	 */
	public byte[] getMutationTimestamp(boolean microsToMillis) {
		long result = timestamp;
		if (microsToMillis)
			result /= 1000;
		return NumberHelper.toBytes(result);
	}
	
	/**
	 * Get the raw time stamp value used by this <code>Mutator</code> instance.
	 * @return							The raw time stamp value being used
	 */
	public long getMutationTimestampValue() {
		return timestamp;
	}
	
	@SuppressWarnings("serial")
	class MutationList extends ArrayList<Mutation> {}
	@SuppressWarnings("serial")
	class MutationsByCf extends HashMap<String, List<Mutation>> {}
	@SuppressWarnings("serial")
	class MutationsByKey extends HashMap<String, Map<String, List<Mutation>>> {}
	
	private final Map<String, Map<String, List<Mutation>>> batch;
	private final long timestamp;
	
	/**
	 * Create a batch mutation operation.
	 * @param keyspace					The keyspace the batch mutation will modify
	 */
	protected Mutator(ThriftPool thrift, String keyspace) {
		this(thrift, keyspace, System.currentTimeMillis() * 1000);
	}
	
	/**
	 * Create a batch mutation operation.
	 * @param keyspace					The keyspace the batch mutation will modify
	 * @param timestamp					The time stamp to use for the operation. This should be in microseconds.
	 */
	protected Mutator(ThriftPool thrift, String keyspace, long timestamp) {
		super(thrift, keyspace);
		this.timestamp = timestamp;
		batch = new MutationsByKey();
	}
		
	private MutationList getMutationList(String key, String colFamily) {
		MutationsByCf mutsByCf = (MutationsByCf) batch.get(key);
		if (mutsByCf == null) {
			mutsByCf = new MutationsByCf();
			batch.put(key, mutsByCf);
		}
		MutationList mutList = (MutationList) mutsByCf.get(colFamily);
		if (mutList == null) {
			mutList = new MutationList();
			mutsByCf.put(colFamily, mutList);
		}
		return mutList;
	}
}
