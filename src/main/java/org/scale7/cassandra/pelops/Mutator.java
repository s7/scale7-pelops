/*
 * The MIT License
 *
 * Copyright (c) 2011 Dominic Williams, Daniel Washusen and contributors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package org.scale7.cassandra.pelops;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.*;
import org.scale7.cassandra.pelops.exceptions.ModelException;
import org.scale7.cassandra.pelops.exceptions.PelopsException;
import org.scale7.cassandra.pelops.pool.IThriftPool;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

import static org.scale7.cassandra.pelops.Bytes.fromUTF8;
import static org.scale7.cassandra.pelops.Bytes.nullSafeGet;

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
     * @throws PelopsException
     */
    public void execute(final ConsistencyLevel cLevel) throws PelopsException {
        IOperation<Void> operation = new IOperation<Void>() {
            @Override
            public Void execute(IThriftPool.IPooledConnection conn) throws Exception {
                // Send batch mutation job to Thrift connection
                conn.getAPI().batch_mutate(batch, cLevel);
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
    public Mutator writeColumn(String colFamily, String rowKey, Column column) {
        writeColumn(colFamily, fromUTF8(rowKey), column);
        return this;
    }

    /**
     * Write a column value.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param column                    The value of the column
     */
    public Mutator writeColumn(String colFamily, Bytes rowKey, Column column) {
        writeColumn(colFamily, rowKey, column, this.deleteIfNull);
        return this;
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
    public Mutator writeColumn(String colFamily, Bytes rowKey, Column column, boolean deleteIfNullValue) {
        if (!deleteIfNullValue) {
            writeColumnInternal(colFamily, rowKey, column);
        } else {
            if (column.isSetValue()) {
                writeColumnInternal(colFamily, rowKey, column);
            } else {
                deleteColumn(colFamily, rowKey, Bytes.fromByteArray(column.getName()));
            }
        }
        return this;
    }

    private void writeColumnInternal(String colFamily, Bytes rowKey, Column column) {
    	validateRowKey(rowKey);
    	validateColumn(column);
        ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
        cosc.setColumn(column);
        Mutation mutation = new Mutation();
        mutation.setColumn_or_supercolumn(cosc);
        getMutationList(colFamily, rowKey).add(mutation);
    }

	/**
     * Write a list of columns to a key
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param columns                   The list of columns to write
     */
    public Mutator writeColumns(String colFamily, String rowKey, List<Column> columns) {
        writeColumns(colFamily, Bytes.fromUTF8(rowKey), columns);
        return this;
    }

    /**
     * Write a list of columns to a key
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param columns                   The list of columns to write
     */
    public Mutator writeColumns(String colFamily, Bytes rowKey, List<Column> columns) {
        for (Column column : columns) {
            writeColumn(colFamily, rowKey, column);
        }
        return this;
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
    public Mutator writeColumns(String colFamily, Bytes rowKey, List<Column> columns, boolean deleteIfNullValue) {
        for (Column column : columns) {
            writeColumn(colFamily, rowKey, column, deleteIfNullValue);
        }
        return this;
    }

    /**
     * Write a single sub-column value to a super column. If wish to write multiple sub-columns for a
     * super column, then it is more efficient to use <code>writeSubColumns</code>
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the super column
     * @param subColumn                 The sub-column
     */
    public Mutator writeSubColumn(String colFamily, String rowKey, String colName, Column subColumn) {
        writeSubColumn(colFamily, rowKey, fromUTF8(colName), subColumn);
        return this;
    }

    /**
     * Write a single sub-column value to a super column. If wish to write multiple sub-columns for a
     * super column, then it is more efficient to use <code>writeSubColumns</code>
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the super column
     * @param subColumn                 The sub-column
     */
    public Mutator writeSubColumn(String colFamily, String rowKey, Bytes colName, Column subColumn) {
        writeSubColumn(colFamily, Bytes.fromUTF8(rowKey), colName, subColumn);
        return this;
    }

    /**
     * Write a single sub-column value to a super column. If you wish to write multiple sub-columns for a
     * super column, then it is more efficient to use <code>writeSubColumns</code>
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the super column
     * @param subColumn                 The sub-column
     */
    public Mutator writeSubColumn(String colFamily, Bytes rowKey, Bytes colName, Column subColumn) {
        writeSubColumns(colFamily, rowKey, colName, Arrays.asList(subColumn));
        return this;
    }

    /**
     * Write multiple sub-column values to a super column.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the super column
     * @param subColumns                A list of the sub-columns to write
     */
    public Mutator writeSubColumns(String colFamily, String rowKey, String colName, List<Column> subColumns) {
        writeSubColumns(colFamily, rowKey, fromUTF8(colName), subColumns);
        return this;
    }

    /**
     * Write multiple sub-column values to a super column.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the super column
     * @param subColumns                A list of the sub-columns to write
     */
    public Mutator writeSubColumns(String colFamily, String rowKey, Bytes colName, List<Column> subColumns) {
        writeSubColumns(colFamily, fromUTF8(rowKey), colName, subColumns);
        return this;
    }

    /**
     * Write multiple sub-column values to a super column.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the super column
     * @param subColumns                A list of the sub-columns to write
     */
    public Mutator writeSubColumns(String colFamily, Bytes rowKey, Bytes colName, List<Column> subColumns) {
        writeSubColumns(colFamily, rowKey, colName, subColumns, this.deleteIfNull);
        return this;
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
    public Mutator writeSubColumns(String colFamily, Bytes rowKey, Bytes colName, List<Column> subColumns, boolean deleteIfNullValue) {
        if (!deleteIfNullValue) {
            writeSubColumnsInternal(colFamily, rowKey, colName, subColumns);
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
                writeSubColumnsInternal(colFamily, rowKey, colName, subColumns);
            } else {
                // separate out the columns that have a value from those that don't
                List<Column> subColumnsWithValue = new ArrayList<Column>(subColumns.size());
                List<Bytes> subColumnsWithoutValue = new ArrayList<Bytes>(subColumns.size());
                for (Column subColumn : subColumns) {
                    if (subColumn.isSetValue()) {
                        subColumnsWithValue.add(subColumn);
                    } else {
                        subColumnsWithoutValue.add(Bytes.fromByteArray(subColumn.getName()));
                    }
                }

                writeSubColumnsInternal(colFamily, rowKey, colName, subColumnsWithValue);
                deleteSubColumns(colFamily, rowKey, colName, subColumnsWithoutValue);
            }
        }
        return this;
    }

    private void writeSubColumnsInternal(String colFamily, Bytes rowKey, Bytes colName, List<Column> subColumns) {
    	validateRowKey(rowKey);
    	validateColumnName(colName);
    	validateColumns(subColumns);
        SuperColumn scol = new SuperColumn(nullSafeGet(colName), subColumns);
        ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
        cosc.setSuper_column(scol);
        Mutation mutation = new Mutation();
        mutation.setColumn_or_supercolumn(cosc);
        getMutationList(colFamily, rowKey).add(mutation);
    }

	/**
     * Delete a column or super column
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the column or super column to delete.
     */
    public Mutator deleteColumn(String colFamily, String rowKey, String colName) {
        deleteColumn(colFamily, rowKey, fromUTF8(colName));
        return this;
    }

    /**
     * Delete a column or super column.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the column or super column to delete.
     */
    public Mutator deleteColumn(String colFamily, String rowKey, Bytes colName) {
        deleteColumn(colFamily, Bytes.fromUTF8(rowKey), colName);
        return this;
    }

    /**
     * Delete a column or super column.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the column or super column to delete.
     */
    public Mutator deleteColumn(String colFamily, Bytes rowKey, Bytes colName) {
        deleteColumns(colFamily, rowKey, Arrays.asList(colName));
        return this;
    }

    /**
     * Delete a list of columns or super columns.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colNames                  The column and/or super column names to delete
     */
    public Mutator deleteColumns(String colFamily, String rowKey, Bytes... colNames) {
        deleteColumns(colFamily, Bytes.fromUTF8(rowKey), Arrays.asList(colNames));
        return this;
    }

    /**
     * Delete a list of columns or super columns.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colNames                  The column and/or super column names to delete
     */
    public Mutator deleteColumns(String colFamily, Bytes rowKey, Bytes... colNames) {
        deleteColumns(colFamily, rowKey, Arrays.asList(colNames));
        return this;
    }

    /**
     * Delete a list of columns or super columns.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colNames                  The column and/or super column names to delete
     */
    public Mutator deleteColumns(String colFamily, String rowKey, String... colNames) {
        List<Bytes> colNameList = new ArrayList<Bytes>(colNames.length);
        for (String colName : colNames)
            colNameList.add(fromUTF8(colName));
        deleteColumns(colFamily, Bytes.fromUTF8(rowKey), colNameList);
        return this;
    }

    /**
     * Delete a list of columns or super columns.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colNames                  The column and/or super column names to delete
     */
    public Mutator deleteColumns(String colFamily, String rowKey, List<Bytes> colNames) {
        deleteColumns(colFamily, Bytes.fromUTF8(rowKey), colNames);
        return this;
    }

    /**
     * Delete a list of columns or super columns.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colNames                  The column and/or super column names to delete
     */
    public Mutator deleteColumns(String colFamily, Bytes rowKey, List<Bytes> colNames) {
    	validateRowKey(rowKey);
    	validateColumnNames(colNames);
        SlicePredicate pred = new SlicePredicate();
        pred.setColumn_names(Bytes.transformBytesToList(colNames));
        Deletion deletion = new Deletion(timestamp);
        deletion.setPredicate(pred);
        Mutation mutation = new Mutation();
        mutation.setDeletion(deletion);
        getMutationList(colFamily, rowKey).add(mutation);
        return this;
    }

    /**
     * Delete a column or super column.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the super column to modify.
     * @param subColName                The name of the sub-column to delete.
     */
    public Mutator deleteSubColumn(String colFamily, String rowKey, String colName, String subColName) {
        deleteSubColumn(colFamily, rowKey, fromUTF8(colName), fromUTF8(subColName));
        return this;
    }

    /**
     * Delete a column or super column.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the super column to modify.
     * @param subColName                The name of the sub-column to delete.
     */
    public Mutator deleteSubColumn(String colFamily, String rowKey, Bytes colName, String subColName) {
        deleteSubColumn(colFamily, rowKey, colName, fromUTF8(subColName));
        return this;
    }

    /**
     * Delete a column or super column.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the super column to modify.
     * @param subColName                The name of the sub-column to delete.
     */
    public Mutator deleteSubColumn(String colFamily, String rowKey, String colName, Bytes subColName) {
        deleteSubColumn(colFamily, rowKey, fromUTF8(colName), subColName);
        return this;
    }

    /**
     * Delete a column or super column.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the super column to modify.
     * @param subColName                The name of the sub-column to delete.
     */
    public Mutator deleteSubColumn(String colFamily, String rowKey, Bytes colName, Bytes subColName) {
        deleteSubColumn(colFamily, Bytes.fromUTF8(rowKey), colName, subColName);
        return this;
    }

    /**
     * Delete a column or super column.
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName                   The name of the super column to modify.
     * @param subColName                The name of the sub-column to delete.
     */
    public Mutator deleteSubColumn(String colFamily, Bytes rowKey, Bytes colName, Bytes subColName) {
        List<Bytes> subColNames = new ArrayList<Bytes>(1);
        subColNames.add(subColName);
        deleteSubColumns(colFamily, rowKey, colName, subColNames);
        return this;
    }

    /**
     * Delete a list of sub-columns
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName               	The name of the super column to modify
     * @param subColNames               The sub-column names to delete (empty value will result in all columns being removed)
     */
    public Mutator deleteSubColumns(String colFamily, String rowKey, String colName, String... subColNames) {
        deleteSubColumns(colFamily, rowKey, fromUTF8(colName), subColNames);
        return this;
    }

    /**
     * Delete a list of sub-columns
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName               	The name of the super column to modify
     * @param subColNames               The sub-column names to delete (empty value will result in all columns being removed)
     */
    public Mutator deleteSubColumns(String colFamily, String rowKey, Bytes colName, String... subColNames) {
        deleteSubColumns(colFamily, Bytes.fromUTF8(rowKey), colName, subColNames);
        return this;
    }

    /**
     * Delete a list of sub-columns
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName               	The name of the super column to modify
     * @param subColNames               The sub-column names to delete (empty value will result in all columns being removed)
     */
    public Mutator deleteSubColumns(String colFamily, Bytes rowKey, Bytes colName, String... subColNames) {
        List<Bytes> subColNamesList = new ArrayList<Bytes>(subColNames.length);
        for (String subColName : subColNames)
            subColNamesList.add(fromUTF8(subColName));
        deleteSubColumns(colFamily, rowKey, colName, subColNamesList);
        return this;
    }

    /**
     * Delete all sub-columns
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName               	The name of the super column to modify
     */
    public Mutator deleteSubColumns(String colFamily, String rowKey, String colName) {
        deleteSubColumns(colFamily, fromUTF8(rowKey), fromUTF8(colName), (List<Bytes>) null);
        return this;
    }

    /**
     * Delete a list of sub-columns
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName               	The name of the super column to modify
     * @param subColNames               The sub-column names to delete
     */
    public Mutator deleteSubColumns(String colFamily, String rowKey, String colName, List<Bytes> subColNames) {
        deleteSubColumns(colFamily, fromUTF8(rowKey), fromUTF8(colName), subColNames);
        return this;
    }

    /**
     * Delete all sub-columns
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName               	The name of the super column to modify
     */
    public Mutator deleteSubColumns(String colFamily, String rowKey, Bytes colName) {
        deleteSubColumns(colFamily, fromUTF8(rowKey), colName, (List<Bytes>) null);
        return this;
    }

    /**
     * Delete a list of sub-columns
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName               	The name of the super column to modify
     * @param subColNames               The sub-column names to delete
     */
    public Mutator deleteSubColumns(String colFamily, String rowKey, Bytes colName, List<Bytes> subColNames) {
        deleteSubColumns(colFamily, fromUTF8(rowKey), colName, subColNames);
        return this;
    }

    /**
     * Delete a list of sub-columns
     * @param colFamily                 The column family
     * @param rowKey                    The key of the row to modify
     * @param colName               	The name of the super column to modify
     * @param subColNames               The sub-column names to delete
     */
    public Mutator deleteSubColumns(String colFamily, Bytes rowKey, Bytes colName, List<Bytes> subColNames) {
    	validateRowKey(rowKey);
    	validateColumnName(colName);
    	validateColumnNames(subColNames);
        Deletion deletion = new Deletion(timestamp);
        deletion.setSuper_column(nullSafeGet(colName));
        // CASSANDRA-1027 allows for a null predicate
        deletion.setPredicate(
                subColNames != null && !subColNames.isEmpty() ?
                        new SlicePredicate().setColumn_names(Bytes.transformBytesToList(subColNames)) : null
        );
        Mutation mutation = new Mutation();
        mutation.setDeletion(deletion);
        getMutationList(colFamily, rowKey).add(mutation);
        return this;
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
     * @param ttl                        The time to live (in seconds) for the column
     * @return                           An appropriate <code>Column</code> object
     */
    public Column newColumn(String colName, String colValue, int ttl) {
        return newColumn(fromUTF8(colName), fromUTF8(colValue), ttl);
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
     * @param ttl                        The time to live (in seconds) for the column
     * @return                           An appropriate <code>Column</code> object
     */
    public Column newColumn(Bytes colName, String colValue, int ttl) {
        return newColumn(colName, fromUTF8(colValue), ttl);
    }

    /**
     * Create new Column object with an empty value and the time stamp passed to the constructor
     * @param colName                    The column name
     * @return                           An appropriate <code>Column</code> object
     */
    public Column newColumn(String colName) {
        return newColumn(fromUTF8(colName), Bytes.EMPTY);
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
     * Create new Column object with an empty value and the time stamp passed to the constructor
     * @param colName                    The column name
     * @param ttl                        The time to live (in seconds) for the column
     * @return                           An appropriate <code>Column</code> object
     */
    public Column newColumn(String colName, int ttl) {
        return newColumn(fromUTF8(colName), Bytes.EMPTY, ttl);
    }

    /**
     * Create new Column object with the time stamp passed to the constructor
     * @param colName                    The column name
     * @param colValue                   The column value
     * @param ttl                        The time to live (in seconds) for the column
     * @return                           An appropriate <code>Column</code> object
     */
    public Column newColumn(String colName, Bytes colValue, int ttl) {
        return newColumn(fromUTF8(colName), colValue, ttl);
    }

    /**
     * Create new Column object with an empty value and the time stamp passed to the constructor
     * @param colName                    The column name
     * @return                           An appropriate <code>Column</code> object
     */
    public Column newColumn(Bytes colName) {
        return newColumn(colName, Bytes.EMPTY);
    }

    /**
     * Create new Column object with the time stamp passed to the constructor
     * @param colName                    The column name
     * @param colValue                   The column value
     * @return                           An appropriate <code>Column</code> object
     */
    public Column newColumn(Bytes colName, Bytes colValue) {
        return newColumn(colName, colValue, this.ttl);
    }

    /**
     * Create new Column object with an empty value and the time stamp passed to the constructor
     * @param colName                    The column name
     * @param ttl                        The time to live (in seconds) for the column
     * @return                           An appropriate <code>Column</code> object
     */
    public Column newColumn(Bytes colName, int ttl) {
        return newColumn(colName, Bytes.EMPTY, ttl);
    }

    /**
     * Create new Column object with the time stamp passed to the constructor
     * @param colName                    The column name
     * @param colValue                   The column value
     * @param ttl                        The time to live (in seconds) for the column (-1 for default)
     * @return                           An appropriate <code>Column</code> object
     */
    public Column newColumn(Bytes colName, Bytes colValue, int ttl) {
        Column column = new Column(nullSafeGet(colName), nullSafeGet(colValue), timestamp);
        if (ttl != NO_TTL) column.setTtl(ttl);
        return column;
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
        long result = timestamp;
        if (microsToMillis)
            result /= 1000;
        return Bytes.fromLong(result);
    }

    /**
     * Get the raw time stamp value used by this <code>Mutator</code> instance.
     * @return                            The raw time stamp value being used
     */
    public long getMutationTimestampValue() {
        return timestamp;
    }

    @SuppressWarnings("serial")
    class MutationList extends ArrayList<Mutation> {}
    @SuppressWarnings("serial")
    class MutationsByCf extends HashMap<String, List<Mutation>> {}
    @SuppressWarnings("serial")
    class MutationsByKey extends HashMap<ByteBuffer, Map<String, List<Mutation>>> {}

    /**
     * Used to indicate that the ttl property on column instances should not be set.
     */
    public static final int NO_TTL = -1;

    private final Map<ByteBuffer, Map<String, List<Mutation>>> batch;
    protected final long timestamp;
    protected final boolean deleteIfNull;
    protected final int ttl;



    /**
     * Create a batch mutation operation.
     */
    public Mutator(IThriftPool thrift) {
        this(thrift, System.currentTimeMillis() * 1000, thrift.getOperandPolicy().isDeleteIfNull());
    }

    /**
     * Create a batch mutation operation.
     * @param timestamp The time stamp to use for the operation.
     */
    public Mutator(IThriftPool thrift, long timestamp, boolean deleteIfNull) {
        this(thrift, timestamp, deleteIfNull, NO_TTL);
    }

    /**
     * Create a batch mutation operation.
     * @param thrift the pool
     * @param timestamp The time stamp to use for the operation.
     * @param deleteIfNull determine if null values on columns will result in a delete
     * @param ttl the ttl (in seconds) that columns created using the various {@link #newColumn(Bytes, Bytes)}
     * helper methods will default to (null to indicate no default)
     */
    public Mutator(IThriftPool thrift, long timestamp, boolean deleteIfNull, int ttl) {
        super(thrift);
        this.timestamp = timestamp;
        this.deleteIfNull = deleteIfNull;
        this.ttl = ttl;
        batch = new MutationsByKey();
    }

    protected Map<ByteBuffer, Map<String, List<Mutation>>> getBatch() {
		return batch;
	}

    protected MutationList getMutationList(String colFamily, Bytes key) {
        ByteBuffer keyBuffer = key.getBytes();
        MutationsByCf mutsByCf = (MutationsByCf) batch.get(keyBuffer);
        if (mutsByCf == null) {
            mutsByCf = new MutationsByCf();
            batch.put(keyBuffer, mutsByCf);
        }
        MutationList mutList = (MutationList) mutsByCf.get(colFamily);
        if (mutList == null) {
            mutList = new MutationList();
            mutsByCf.put(colFamily, mutList);
        }
        return mutList;
    }

    private void validateRowKey(Bytes rowKey) {
    	if (rowKey == null || rowKey.isNull())
    		throw new ModelException("Row Key is null");
    }

    private void validateColumn(Column column) {
    	if (!column.isSetName())
    		throw new ModelException("Column name is null");
    	if (!column.isSetValue())
    		throw new ModelException("Column value is null");
	}

    private void validateColumns(List<Column> columns) {
    	for (Column c : columns) validateColumn(c);
	}

    private void validateColumnNames(List<Bytes> names) {
    	for (Bytes n : names) validateColumnName(n);
	}

	private void validateColumnName(Bytes name) {
		if (name.isNull())
			throw new ModelException("Column name is null");
	}
}
