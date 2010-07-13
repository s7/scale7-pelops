package org.wyki.cassandra.pelops;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.*;
import org.wyki.cassandra.pelops.ThriftPool.Connection;

import static org.wyki.cassandra.pelops.Bytes.from;
import static org.wyki.cassandra.pelops.Bytes.nullSafeGet;
import static org.wyki.cassandra.pelops.Bytes.transform;

/**
 * Facilitates the mutation of data within a Cassandra keyspace: the desired mutations should first be specified by
 * calling methods such as <code>writeColumn(...)</code>, which should then be sent to Cassandra in a single batch by
 * calling <code>execute(...)</code>. After the desired batch of mutations has been executed, the <code>Mutator</code>
 * object can not be re-used.
 *
 * @author dominicwilliams
 *
 */
public class Mutator extends Operand implements Operand.KeyspaceAware {

    /**
     * Execute the mutations that have been specified by sending them to Cassandra in a single batch.
     * @param cLevel                    The Cassandra consistency level to be used
     * @throws Exception
     */
    public void execute(final ConsistencyLevel cLevel) throws Exception {
        final HashMap<byte[], Map<String, List<Mutation>>> convertedBatch = new HashMap<byte[], Map<String, List<Mutation>>>(batch.size());
        for (Map.Entry<Bytes, Map<String, List<Mutation>>> batchEntry : batch.entrySet()) {
            convertedBatch.put(batchEntry.getKey().getBytes(), batchEntry.getValue());
        }

        IOperation operation = new IOperation() {
            @Override
            public Object execute(Connection conn) throws Exception {
                // Send batch mutation job to Thrift connection
                conn.getAPI().batch_mutate(convertedBatch, cLevel);
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
     * @param rowKey                    The key of the row to modify
     * @param colFamily                 The name of the column family to modify
     * @param column                    The value of the column
     */
    public void writeColumn(String rowKey, String colFamily, Column column) {
        writeColumn(from(rowKey), colFamily, column);
    }

    /**
     * Write a column value.
     * @param rowKey                    The key of the row to modify
     * @param colFamily                 The name of the column family to modify
     * @param column                    The value of the column
     */
    public void writeColumn(Bytes rowKey, String colFamily, Column column) {
        ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
        cosc.setColumn(column);
        Mutation mutation = new Mutation();
        mutation.setColumn_or_supercolumn(cosc);
        getMutationList(rowKey, colFamily).add(mutation);
    }

    /**
     * Write a list of columns to a key
     * @param rowKey                    The key of the row to modify
     * @param colFamily                 The name of the column family to modify
     * @param columns                   The list of columns to write
     */
    public void writeColumns(String rowKey, String colFamily, List<Column> columns) {
        for (Column column : columns) {
            writeColumn(rowKey, colFamily, column);
        }
    }

    /**
     * Write a single sub-column value to a super column. If wish to write multiple sub-columns for a
     * super column, then it is more efficient to use <code>writeSubColumns</code>
     * @param rowKey                    The key of the row to modify
     * @param colFamily                 The name of the super column family to operate on
     * @param colName                   The name of the super column
     * @param subColumn                 The sub-column
     */
    public void writeSubColumn(String rowKey, String colFamily, String colName, Column subColumn) {
        writeSubColumn(rowKey, colFamily, from(colName), subColumn);
    }

    /**
     * Write a single sub-column value to a super column. If wish to write multiple sub-columns for a
     * super column, then it is more efficient to use <code>writeSubColumns</code>
     * @param rowKey                    The key of the row to modify
     * @param colFamily                 The name of the super column family to operate on
     * @param colName                   The name of the super column
     * @param subColumn                 The sub-column
     */
    public void writeSubColumn(String rowKey, String colFamily, Bytes colName, Column subColumn) {
        writeSubColumns(rowKey, colFamily, colName, Arrays.asList(subColumn));
    }

    /**
     * Write multiple sub-column values to a super column.
     * @param rowKey                    The key of the row to modify
     * @param colFamily                 The name of the super column family to operate on
     * @param colName                   The name of the super column
     * @param subColumns                A list of the sub-columns to write
     */
    public void writeSubColumns(String rowKey, String colFamily, String colName, List<Column> subColumns) {
        writeSubColumns(rowKey, colFamily, from(colName), subColumns);
    }

    /**
     * Write multiple sub-column values to a super column.
     * @param rowKey                    The key of the row to modify
     * @param colFamily                 The name of the super column family to operate on
     * @param colName                   The name of the super column
     * @param subColumns                A list of the sub-columns to write
     */
    public void writeSubColumns(String rowKey, String colFamily, Bytes colName, List<Column> subColumns) {
        writeSubColumns(from(rowKey), colFamily, colName, subColumns);
    }

    /**
     * Write multiple sub-column values to a super column.
     * @param rowKey                    The key of the row to modify
     * @param colFamily                 The name of the super column family to operate on
     * @param colName                   The name of the super column
     * @param subColumns                A list of the sub-columns to write
     */
    public void writeSubColumns(Bytes rowKey, String colFamily, Bytes colName, List<Column> subColumns) {
        SuperColumn scol = new SuperColumn(nullSafeGet(colName), subColumns);
        ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
        cosc.setSuper_column(scol);
        Mutation mutation = new Mutation();
        mutation.setColumn_or_supercolumn(cosc);
        getMutationList(rowKey, colFamily).add(mutation);
    }

    /**
     * Delete a column or super column
     * @param rowKey                    The key of the row to modify
     * @param colFamily                 The name of the column family to modify
     * @param colName                   The name of the column or super column to delete.
     */
    public void deleteColumn(String rowKey, String colFamily, String colName) {
        deleteColumn(rowKey, colFamily, from(colName));
    }

    /**
     * Delete a column or super column.
     * @param rowKey                    The key of the row to modify
     * @param colFamily                 The name of the column family to modify
     * @param colName                   The name of the column or super column to delete.
     */
    public void deleteColumn(String rowKey, String colFamily, Bytes colName) {
        deleteColumns(rowKey, colFamily, Arrays.asList(colName));
    }

    /**
     * Delete a list of columns or super columns.
     * @param rowKey                    The key of the row to modify
     * @param colFamily                 The name of the column family to modify
     * @param colNames                  The column and/or super column names to delete
     */
    public void deleteColumns(String rowKey, String colFamily, Bytes... colNames) {
        deleteColumns(rowKey, colFamily, Arrays.asList(colNames));
    }

    /**
     * Delete a list of columns or super columns.
     * @param rowKey                    The key of the row to modify
     * @param colFamily                 The name of the column family to modify
     * @param colNames                  The column and/or super column names to delete
     */
    public void deleteColumns(String rowKey, String colFamily, String... colNames) {
        List<Bytes> colNameList = new ArrayList<Bytes>(colNames.length);
        for (String colName : colNames)
            colNameList.add(from(colName));
        deleteColumns(rowKey, colFamily, colNameList);
    }

    /**
     * Delete a list of columns or super columns.
     * @param rowKey                    The key of the row to modify
     * @param colFamily                 The name of the column family to modify
     * @param colNames                  The column and/or super column names to delete
     */
    public void deleteColumns(String rowKey, String colFamily, List<Bytes> colNames) {
        SlicePredicate pred = new SlicePredicate();
        pred.setColumn_names(transform(colNames));
        Deletion deletion = new Deletion(clock);
        deletion.setPredicate(pred);
        Mutation mutation = new Mutation();
        mutation.setDeletion(deletion);
        getMutationList(from(rowKey), colFamily).add(mutation);
    }

    /**
     * Delete a column or super column.
     * @param rowKey                    The key of the row to modify
     * @param colFamily                 The name of the column family to modify
     * @param colName                   The name of the super column to modify.
     * @param subColName                The name of the sub-column to delete.
     */
    public void deleteSubColumn(String rowKey, String colFamily, String colName, String subColName) {
        deleteSubColumn(rowKey, colFamily, from(colName), from(subColName));
    }

    /**
     * Delete a column or super column.
     * @param rowKey                    The key of the row to modify
     * @param colFamily                 The name of the column family to modify
     * @param colName                   The name of the super column to modify.
     * @param subColName                The name of the sub-column to delete.
     */
    public void deleteSubColumn(String rowKey, String colFamily, Bytes colName, String subColName) {
        deleteSubColumn(rowKey, colFamily, colName, from(subColName));
    }

    /**
     * Delete a column or super column.
     * @param rowKey                    The key of the row to modify
     * @param colFamily                 The name of the column family to modify
     * @param colName                   The name of the super column to modify.
     * @param subColName                The name of the sub-column to delete.
     */
    public void deleteSubColumn(String rowKey, String colFamily, String colName, Bytes subColName) {
        deleteSubColumn(rowKey, colFamily, from(colName), subColName);
    }

    /**
     * Delete a column or super column.
     * @param rowKey                    The key of the row to modify
     * @param colFamily                 The name of the column family to modify
     * @param colName                   The name of the super column to modify.
     * @param subColName                The name of the sub-column to delete.
     */
    public void deleteSubColumn(String rowKey, String colFamily, Bytes colName, Bytes subColName) {
        List<Bytes> subColNames = new ArrayList<Bytes>(1);
        subColNames.add(subColName);
        deleteSubColumns(rowKey, colFamily, colName, subColNames);
    }

    /**
     * Delete a list of sub-columns
     * @param rowKey                    The key of the row to modify
     * @param colFamily                 The name of the column family to modify
     * @param colName               The name of the super column to modify
     * @param subColNames               The sub-column names to delete (empty value will result in all columns being removed)
     */
    public void deleteSubColumns(String rowKey, String colFamily, String colName, String... subColNames) {
        deleteSubColumns(rowKey, colFamily, from(colName), subColNames);
    }

    /**
     * Delete a list of sub-columns
     * @param rowKey                    The key of the row to modify
     * @param colFamily                 The name of the column family to modify
     * @param colName               The name of the super column to modify
     * @param subColNames               The sub-column names to delete (empty value will result in all columns being removed)
     */
    public void deleteSubColumns(String rowKey, String colFamily, Bytes colName, String... subColNames) {
        List<Bytes> subColNamesList = new ArrayList<Bytes>(subColNames.length);
        for (String subColName : subColNames)
            subColNamesList.add(from(subColName));
        deleteSubColumns(rowKey, colFamily, colName, subColNamesList);
    }

    /**
     * Delete all sub-columns
     * @param rowKey                    The key of the row to modify
     * @param colFamily                 The name of the column family to modify
     * @param colName               The name of the super column to modify
     */
    public void deleteSubColumns(String rowKey, String colFamily, String colName) {
        deleteSubColumns(from(rowKey), colFamily, from(colName), (List<Bytes>) null);
    }

    /**
     * Delete a list of sub-columns
     * @param rowKey                    The key of the row to modify
     * @param colFamily                 The name of the column family to modify
     * @param colName               The name of the super column to modify
     * @param subColNames               The sub-column names to delete
     */
    public void deleteSubColumns(String rowKey, String colFamily, String colName, List<Bytes> subColNames) {
        deleteSubColumns(from(rowKey), colFamily, from(colName), subColNames);
    }

    /**
     * Delete all sub-columns
     * @param rowKey                    The key of the row to modify
     * @param colFamily                 The name of the column family to modify
     * @param colName               The name of the super column to modify
     */
    public void deleteSubColumns(String rowKey, String colFamily, Bytes colName) {
        deleteSubColumns(from(rowKey), colFamily, colName, (List<Bytes>) null);
    }

    /**
     * Delete a list of sub-columns
     * @param rowKey                    The key of the row to modify
     * @param colFamily                 The name of the column family to modify
     * @param colName               The name of the super column to modify
     * @param subColNames               The sub-column names to delete
     */
    public void deleteSubColumns(String rowKey, String colFamily, Bytes colName, List<Bytes> subColNames) {
        deleteSubColumns(from(rowKey), colFamily, colName, subColNames);
    }

    /**
     * Delete a list of sub-columns
     * @param rowKey                    The key of the row to modify
     * @param colFamily                 The name of the column family to modify
     * @param colName               The name of the super column to modify
     * @param subColNames               The sub-column names to delete
     */
    public void deleteSubColumns(Bytes rowKey, String colFamily, Bytes colName, List<Bytes> subColNames) {
        Deletion deletion = new Deletion(clock);
        deletion.setSuper_column(nullSafeGet(colName));
        // CASSANDRA-1027 allows for a null predicate
        deletion.setPredicate(
                subColNames != null && !subColNames.isEmpty() ?
                        new SlicePredicate().setColumn_names(transform(subColNames)) : null
        );
        Mutation mutation = new Mutation();
        mutation.setDeletion(deletion);
        getMutationList(rowKey, colFamily).add(mutation);
    }

    /**
     * Create new Column object with the time stamp passed to the constructor
     * @param colName                    The column name
     * @param colValue                   The column value
     * @return                           An appropriate <code>Column</code> object
     */
    public Column newColumn(String colName, String colValue) {
        return newColumn(from(colName), from(colValue));
    }

    /**
     * Create new Column object with the time stamp passed to the constructor
     * @param colName                    The column name
     * @param colValue                   The column value
     * @return                           An appropriate <code>Column</code> object
     */
    public Column newColumn(Bytes colName, String colValue) {
        return newColumn(colName, from(colValue));
    }

    /**
     * Create new Column object with the time stamp passed to the constructor
     * @param colName                    The column name
     * @param colValue                   The column value
     * @return                           An appropriate <code>Column</code> object
     */
    public Column newColumn(String colName, Bytes colValue) {
        return newColumn(from(colName), colValue);
    }

    /**
     * Create new Column object with the time stamp passed to the constructor
     * @param colName                    The column name
     * @param colValue                   The column value
     * @return                           An appropriate <code>Column</code> object
     */
    public Column newColumn(Bytes colName, Bytes colValue) {
        return new Column(nullSafeGet(colName), nullSafeGet(colValue), clock);
    }

    /**
     * Create a list of <code>Column</code> objects.
     * @param columns                    The columns from which to compose the list
     * @return                           A list of <code>Column</code> objects
     */
    public List<Column> newColumnList(Column... columns) {
        ArrayList<Column> list = new ArrayList<Column>(columns.length);
        for (Column column : columns)
            list.add(column);
        return list;
    }

    /**
     * Get the default time stamp used by this <code>Mutator</code> instance as a byte[].
     * @param microsToMillis             If the time stamp is UTC microseconds (as is a self-constructed time stamp), whether to convert this into a standard milliseconds value
     * @return                           A byte array containing the time stamp <code>long</code> value
     */
    public byte[] getMutationTimestamp(boolean microsToMillis) {
        long result = clock.getTimestamp();
        if (microsToMillis)
            result /= 1000;
        return NumberHelper.toBytes(result);
    }

    /**
     * Get the raw time stamp value used by this <code>Mutator</code> instance.
     * @return                            The raw time stamp value being used
     */
    public long getMutationTimestampValue() {
        return clock.getTimestamp();
    }

    @SuppressWarnings("serial")
    class MutationList extends ArrayList<Mutation> {}
    @SuppressWarnings("serial")
    class MutationsByCf extends HashMap<String, List<Mutation>> {}
    @SuppressWarnings("serial")
    class MutationsByKey extends HashMap<Bytes, Map<String, List<Mutation>>> {}

    private final Map<Bytes, Map<String, List<Mutation>>> batch;
    private String keyspace;
    private final Clock clock;

    /**
     * Create a batch mutation operation.
     * @param keyspace                    The keyspace the batch mutation will modify
     */
    protected Mutator(ThriftPool thrift, String keyspace) {
        this(thrift, keyspace, new Clock(System.currentTimeMillis() * 1000));
    }

    /**
     * Create a batch mutation operation.
     * @param keyspace                    The keyspace the batch mutation will modify
     * @param clock                   The clock that encapsulates the time stamp to use for the operation.
     *                                This should be in microseconds.
     */
    protected Mutator(ThriftPool thrift, String keyspace, Clock clock) {
        super(thrift);
        this.keyspace = keyspace;
        this.clock = clock;
        batch = new MutationsByKey();
    }

    public String getKeyspace() {
        return keyspace;
    }

    private MutationList getMutationList(Bytes key, String colFamily) {
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
