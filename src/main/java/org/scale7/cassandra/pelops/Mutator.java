package org.scale7.cassandra.pelops;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.*;
import org.scale7.cassandra.pelops.IThriftPool.IConnection;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

import static org.scale7.cassandra.pelops.Bytes.fromUTF8;
import static org.scale7.cassandra.pelops.Bytes.nullSafeGet;
import static org.scale7.cassandra.pelops.Bytes.transformBytesToSet;

/**
 * Facilitates the mutation of data within a Cassandra keyspace: the desired mutations should first be specified by
 * calling methods such as <code>writeColumn(...)</code>, which should then be sent to Cassandra in a single batch by
 * calling <code>execute(...)</code>. After the desired batch of mutations has been executed, the <code>Mutator</code>
 * object can not be re-used.
 *
 * @author dominicwilliams
 *
 */
public class Mutator extends Operand {
    private static final Logger logger = SystemProxy.getLoggerFromFactory(Mutator.class);

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

        IOperation<Void> operation = new IOperation<Void>() {
            @Override
            public Void execute(IConnection conn) throws Exception {
                // Send batch mutation job to Thrift connection
                conn.getAPI().batch_mutate(convertedBatch, cLevel);
                // Nothing to return
                return null;
            }
        };
        tryOperation(operation);
    }

    /**
     * Write a column value.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param column                    The value of the column
     */
    public void writeColumn(String colFamily, String rowKey, Column column) {
        writeColumn(colFamily, fromUTF8(rowKey), column);
    }

    /**
     * Write a column value.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param column                    The value of the column
     */
    public void writeColumn(String colFamily, Bytes rowKey, Column column) {
        ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
        cosc.setColumn(column);
        Mutation mutation = new Mutation();
        mutation.setColumn_or_supercolumn(cosc);
        getMutationList(colFamily, rowKey).add(mutation);
    }

    /**
     * Write a column value.  This method will automatically issue deletes if the deleteIfNullValue is true and the
     * provided column does not have a value.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param column                    The value of the column
     * @param deleteIfNullValue         If true and the provided column does NOT have value (as determined by the
     *                                  {@link org.apache.cassandra.thrift.Column#isSetValue()} method) then issue a
     *                                  {@link #deleteColumn(String, Bytes, Bytes) delete} instead.
     */
    public void writeColumn(String colFamily, Bytes rowKey, Column column, boolean deleteIfNullValue) {
        if (!deleteIfNullValue) {
            writeColumn(colFamily, rowKey, column);
        } else {
            if (column.isSetValue()) {
                writeColumn(colFamily, rowKey, column);
            } else {
                deleteColumn(colFamily, rowKey, Bytes.fromBytes(column.getName()));
            }
        }
    }

    /**
     * Write a list of columns to a key
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param columns                   The list of columns to write
     */
    public void writeColumns(String colFamily, String rowKey, List<Column> columns) {
        writeColumns(colFamily, Bytes.fromUTF8(rowKey), columns);
    }

    /**
     * Write a list of columns to a key
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param columns                   The list of columns to write
     */
    public void writeColumns(String colFamily, Bytes rowKey, List<Column> columns) {
        for (Column column : columns) {
            writeColumn(colFamily, rowKey, column);
        }
    }

    /**
     * Write a list of columns to a key.  This method will automatically issue deletes if the deleteIfNullValue is true and the
     * provided column does not have a value.
     *
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param columns                   The list of columns to write
     * @param deleteIfNullValue         If true and if the provided columns do NOT have value (as determined by the
     *                                  {@link org.apache.cassandra.thrift.Column#isSetValue()} method) then issue a
     *                                  {@link #deleteColumn(String, Bytes, Bytes) delete} instead.
     */
    public void writeColumns(String colFamily, Bytes rowKey, List<Column> columns, boolean deleteIfNullValue) {
        for (Column column : columns) {
            writeColumn(colFamily, rowKey, column, deleteIfNullValue);
        }
    }

    /**
     * Write a single sub-column value to a super column. If wish to write multiple sub-columns for a
     * super column, then it is more efficient to use <code>writeSubColumns</code>
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the super column
     * @param subColumn                 The sub-column
     */
    public void writeSubColumn(String colFamily, String rowKey, String colName, Column subColumn) {
        writeSubColumn(colFamily, rowKey, fromUTF8(colName), subColumn);
    }

    /**
     * Write a single sub-column value to a super column. If wish to write multiple sub-columns for a
     * super column, then it is more efficient to use <code>writeSubColumns</code>
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the super column
     * @param subColumn                 The sub-column
     */
    public void writeSubColumn(String colFamily, String rowKey, Bytes colName, Column subColumn) {
        writeSubColumn(colFamily, Bytes.fromUTF8(rowKey), colName, subColumn);
    }

    /**
     * Write a single sub-column value to a super column. If you wish to write multiple sub-columns for a
     * super column, then it is more efficient to use <code>writeSubColumns</code>
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the super column
     * @param subColumn                 The sub-column
     */
    public void writeSubColumn(String colFamily, Bytes rowKey, Bytes colName, Column subColumn) {
        writeSubColumns(colFamily, rowKey, colName, Arrays.asList(subColumn));
    }

    /**
     * Write multiple sub-column values to a super column.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the super column
     * @param subColumns                A list of the sub-columns to write
     */
    public void writeSubColumns(String colFamily, String rowKey, String colName, List<Column> subColumns) {
        writeSubColumns(colFamily, rowKey, fromUTF8(colName), subColumns);
    }

    /**
     * Write multiple sub-column values to a super column.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the super column
     * @param subColumns                A list of the sub-columns to write
     */
    public void writeSubColumns(String colFamily, String rowKey, Bytes colName, List<Column> subColumns) {
        writeSubColumns(colFamily, fromUTF8(rowKey), colName, subColumns);
    }

    /**
     * Write multiple sub-column values to a super column.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the super column
     * @param subColumns                A list of the sub-columns to write
     */
    public void writeSubColumns(String colFamily, Bytes rowKey, Bytes colName, List<Column> subColumns) {
        SuperColumn scol = new SuperColumn(nullSafeGet(colName), subColumns);
        ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
        cosc.setSuper_column(scol);
        Mutation mutation = new Mutation();
        mutation.setColumn_or_supercolumn(cosc);
        getMutationList(colFamily, rowKey).add(mutation);
    }

    /**
     * Write multiple sub-column values to a super column.  This method will automatically delete sub columns if the
     * deleteIfNullValue is true and any of the sub columns do not have a value.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the super column
     * @param subColumns                A list of the sub-columns to write
     * @param deleteIfNullValue         If true and if the provided columns do NOT have values (as determined by the
     *                                  {@link org.apache.cassandra.thrift.Column#isSetValue()} method) then issue a
     *                                  call to {@link #deleteSubColumns(String, String, Bytes)} with the columns that
     *                                  have no values.
     */
    public void writeSubColumns(String colFamily, Bytes rowKey, Bytes colName, List<Column> subColumns, boolean deleteIfNullValue) {
        if (!deleteIfNullValue) {
            writeSubColumns(colFamily, rowKey, colName, subColumns);
        } else {
            // figure out if we need to worry about columns with empty values
            boolean isEmptyColumnPresent = false;
            for (Column subColumn : subColumns) {
                if (!subColumn.isSetValue()) {
                    isEmptyColumnPresent = true;
                    break;
                }
            }

            if (!isEmptyColumnPresent) {
                writeSubColumns(colFamily, rowKey, colName, subColumns);
            } else {
                // separate out the columns that have a value from those that don't
                List<Column> subColumnsWithValue = new ArrayList<Column>(subColumns.size());
                List<Bytes> subColumnsWithoutValue = new ArrayList<Bytes>(subColumns.size());
                for (Column subColumn : subColumns) {
                    if (subColumn.isSetValue()) {
                        subColumnsWithValue.add(subColumn);
                    } else {
                        subColumnsWithoutValue.add(Bytes.fromBytes(subColumn.getName()));
                    }
                }

                writeSubColumns(colFamily, rowKey, colName, subColumnsWithValue);
                deleteSubColumns(colFamily, rowKey, colName, subColumnsWithoutValue);
            }
        }
    }

    /**
     * Delete a column or super column
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the column or super column to delete.
     */
    public void deleteColumn(String colFamily, String rowKey, String colName) {
        deleteColumn(colFamily, rowKey, fromUTF8(colName));
    }

    /**
     * Delete a column or super column.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the column or super column to delete.
     */
    public void deleteColumn(String colFamily, String rowKey, Bytes colName) {
        deleteColumn(colFamily, Bytes.fromUTF8(rowKey), colName);
    }

    /**
     * Delete a column or super column.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the column or super column to delete.
     */
    public void deleteColumn(String colFamily, Bytes rowKey, Bytes colName) {
        deleteColumns(colFamily, rowKey, Arrays.asList(colName));
    }

    /**
     * Delete a list of columns or super columns.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colNames                  The column and/or super column names to delete
     */
    public void deleteColumns(String colFamily, String rowKey, Bytes... colNames) {
        deleteColumns(colFamily, rowKey, Arrays.asList(colNames));
    }

    /**
     * Delete a list of columns or super columns.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colNames                  The column and/or super column names to delete
     */
    public void deleteColumns(String colFamily, String rowKey, String... colNames) {
        List<Bytes> colNameList = new ArrayList<Bytes>(colNames.length);
        for (String colName : colNames)
            colNameList.add(fromUTF8(colName));
        deleteColumns(colFamily, rowKey, colNameList);
    }

    /**
     * Delete a list of columns or super columns.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colNames                  The column and/or super column names to delete
     */
    public void deleteColumns(String colFamily, String rowKey, List<Bytes> colNames) {
        deleteColumns(colFamily, Bytes.fromUTF8(rowKey), colNames);
    }

    /**
     * Delete a list of columns or super columns.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colNames                  The column and/or super column names to delete
     */
    public void deleteColumns(String colFamily, Bytes rowKey, List<Bytes> colNames) {
        SlicePredicate pred = new SlicePredicate();
        pred.setColumn_names(Bytes.transformBytesToList(colNames));
        Deletion deletion = new Deletion(clock);
        deletion.setPredicate(pred);
        Mutation mutation = new Mutation();
        mutation.setDeletion(deletion);
        getMutationList(colFamily, rowKey).add(mutation);
    }

    /**
     * Delete a column or super column.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the super column to modify.
     * @param subColName                The name of the sub-column to delete.
     */
    public void deleteSubColumn(String colFamily, String rowKey, String colName, String subColName) {
        deleteSubColumn(colFamily, rowKey, fromUTF8(colName), fromUTF8(subColName));
    }

    /**
     * Delete a column or super column.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the super column to modify.
     * @param subColName                The name of the sub-column to delete.
     */
    public void deleteSubColumn(String colFamily, String rowKey, Bytes colName, String subColName) {
        deleteSubColumn(colFamily, rowKey, colName, fromUTF8(subColName));
    }

    /**
     * Delete a column or super column.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the super column to modify.
     * @param subColName                The name of the sub-column to delete.
     */
    public void deleteSubColumn(String colFamily, String rowKey, String colName, Bytes subColName) {
        deleteSubColumn(colFamily, rowKey, fromUTF8(colName), subColName);
    }

    /**
     * Delete a column or super column.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the super column to modify.
     * @param subColName                The name of the sub-column to delete.
     */
    public void deleteSubColumn(String colFamily, String rowKey, Bytes colName, Bytes subColName) {
        deleteSubColumn(colFamily, Bytes.fromUTF8(rowKey), colName, subColName);
    }

    /**
     * Delete a column or super column.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the super column to modify.
     * @param subColName                The name of the sub-column to delete.
     */
    public void deleteSubColumn(String colFamily, Bytes rowKey, Bytes colName, Bytes subColName) {
        List<Bytes> subColNames = new ArrayList<Bytes>(1);
        subColNames.add(subColName);
        deleteSubColumns(colFamily, rowKey, colName, subColNames);
    }

    /**
     * Delete a list of sub-columns
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName               	The name of the super column to modify
     * @param subColNames               The sub-column names to delete (empty value will result in all columns being removed)
     */
    public void deleteSubColumns(String colFamily, String rowKey, String colName, String... subColNames) {
        deleteSubColumns(colFamily, rowKey, fromUTF8(colName), subColNames);
    }

    /**
     * Delete a list of sub-columns
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName               	The name of the super column to modify
     * @param subColNames               The sub-column names to delete (empty value will result in all columns being removed)
     */
    public void deleteSubColumns(String colFamily, String rowKey, Bytes colName, String... subColNames) {
        deleteSubColumns(colFamily, Bytes.fromUTF8(rowKey), colName, subColNames);
    }

    /**
     * Delete a list of sub-columns
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName               	The name of the super column to modify
     * @param subColNames               The sub-column names to delete (empty value will result in all columns being removed)
     */
    public void deleteSubColumns(String colFamily, Bytes rowKey, Bytes colName, String... subColNames) {
        List<Bytes> subColNamesList = new ArrayList<Bytes>(subColNames.length);
        for (String subColName : subColNames)
            subColNamesList.add(fromUTF8(subColName));
        deleteSubColumns(colFamily, rowKey, colName, subColNamesList);
    }

    /**
     * Delete all sub-columns
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName               	The name of the super column to modify
     */
    public void deleteSubColumns(String colFamily, String rowKey, String colName) {
        deleteSubColumns(colFamily, fromUTF8(rowKey), fromUTF8(colName), (List<Bytes>) null);
    }

    /**
     * Delete a list of sub-columns
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName               	The name of the super column to modify
     * @param subColNames               The sub-column names to delete
     */
    public void deleteSubColumns(String colFamily, String rowKey, String colName, List<Bytes> subColNames) {
        deleteSubColumns(colFamily, fromUTF8(rowKey), fromUTF8(colName), subColNames);
    }

    /**
     * Delete all sub-columns
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName               	The name of the super column to modify
     */
    public void deleteSubColumns(String colFamily, String rowKey, Bytes colName) {
        deleteSubColumns(colFamily, fromUTF8(rowKey), colName, (List<Bytes>) null);
    }

    /**
     * Delete a list of sub-columns
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName               	The name of the super column to modify
     * @param subColNames               The sub-column names to delete
     */
    public void deleteSubColumns(String colFamily, String rowKey, Bytes colName, List<Bytes> subColNames) {
        deleteSubColumns(colFamily, fromUTF8(rowKey), colName, subColNames);
    }

    /**
     * Delete a list of sub-columns
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName               	The name of the super column to modify
     * @param subColNames               The sub-column names to delete
     */
    public void deleteSubColumns(String colFamily, Bytes rowKey, Bytes colName, List<Bytes> subColNames) {
        Deletion deletion = new Deletion(clock);
        deletion.setSuper_column(nullSafeGet(colName));
        // CASSANDRA-1027 allows for a null predicate
        deletion.setPredicate(
                subColNames != null && !subColNames.isEmpty() ?
                        new SlicePredicate().setColumn_names(Bytes.transformBytesToList(subColNames)) : null
        );
        Mutation mutation = new Mutation();
        mutation.setDeletion(deletion);
        getMutationList(colFamily, rowKey).add(mutation);
    }

    /**
     * Create new Column object with the time stamp passed to the constructor
     * @param colName                    The column name
     * @param colValue                   The column value
     * @return                           An appropriate <code>Column</code> object
     */
    public Column newColumn(String colName, String colValue) {
        return newColumn(fromUTF8(colName), fromUTF8(colValue));
    }

    /**
     * Create new Column object with the time stamp passed to the constructor
     * @param colName                    The column name
     * @param colValue                   The column value
     * @return                           An appropriate <code>Column</code> object
     */
    public Column newColumn(Bytes colName, String colValue) {
        return newColumn(colName, fromUTF8(colValue));
    }

    /**
     * Create new Column object with the time stamp passed to the constructor
     * @param colName                    The column name
     * @param colValue                   The column value
     * @return                           An appropriate <code>Column</code> object
     */
    public Column newColumn(String colName, Bytes colValue) {
        return newColumn(fromUTF8(colName), colValue);
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
    public Bytes getMutationTimestamp(boolean microsToMillis) {
        long result = clock.getTimestamp();
        if (microsToMillis)
            result /= 1000;
        return Bytes.fromLong(result);
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
    private final Clock clock;

    /**
     * Create a batch mutation operation.
     */
    protected Mutator(IThriftPool thrift) {
        this(thrift, new Clock(System.currentTimeMillis() * 1000));
    }

    /**
     * Create a batch mutation operation.
     * @param clock                   The clock that encapsulates the time stamp to use for the operation.
     */
    protected Mutator(IThriftPool thrift, Clock clock) {
        super(thrift);
        this.clock = clock;
        batch = new MutationsByKey();
    }

    private MutationList getMutationList(String colFamily, Bytes key) {
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
