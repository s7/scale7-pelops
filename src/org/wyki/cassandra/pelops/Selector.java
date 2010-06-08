package org.wyki.cassandra.pelops;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.wyki.cassandra.pelops.ThriftPool.Connection;

import static org.wyki.cassandra.pelops.StringHelper.toBytes;
import static org.wyki.cassandra.pelops.StringHelper.toUTF8;

/**
 * Facilitates the selective retrieval of column data from rows in a Cassandra keyspace.
 *
 * @author dominicwilliams
 *
 */
public class Selector extends KeyspaceOperand {

    /**
     * Get the count of columns in a row.
     * @param rowKey                        The key of the row
     * @param columnFamily                  The name of the column family containing the columns
     * @param cLevel                        The Cassandra consistency level with which to perform the operation
     * @return                              The count of the columns
     * @throws Exception
     */
    public int getColumnCount(String rowKey, String columnFamily, ConsistencyLevel cLevel) throws Exception {
        return getColumnCount(rowKey, newColumnParent(columnFamily), cLevel);
    }

    /**
     * Get the count of sub-columns inside a super column in a row.
     * @param rowKey                        The key of the row
     * @param columnFamily                  The name of the column family containing the columns
     * @param superColName                  The name of the super column
     * @param cLevel                        The Cassandra consistency level with which to perform the operation
     * @return                              The count of the sub-columns
     * @throws Exception
     */
    public int getSubColumnCount(String rowKey, String columnFamily, byte[] superColName, ConsistencyLevel cLevel) throws Exception {
        return getColumnCount(rowKey, newColumnParent(columnFamily, superColName), cLevel);
    }

    /**
     * Get the count of sub-columns inside a super column in a row.
     * @param rowKey                        The key of the row
     * @param columnFamily                  The name of the column family containing the columns
     * @param superColName                  The name of the super column
     * @param cLevel                        The Cassandra consistency level with which to perform the operation
     * @return                              The count of the sub-columns
     * @throws Exception
     */
    public int getSubColumnCount(String rowKey, String columnFamily, String superColName, ConsistencyLevel cLevel) throws Exception {
        return getColumnCount(rowKey, newColumnParent(columnFamily, superColName), cLevel);
    }

    /**
     * Get the count of super columns in a row.
     * @param rowKey                        The key of the row
     * @param columnFamily                  The column family containing the super columns
     * @param cLevel                        The Cassandra consistency level with which to perform the operation
     * @return                              The count of the super columns
     * @throws Exception
     */
    public int getSuperColumnCount(String rowKey, String columnFamily, ConsistencyLevel cLevel) throws Exception {
        return getColumnCount(rowKey, newColumnParent(columnFamily), cLevel);
    }

    private int getColumnCount(final String rowKey, final ColumnParent colParent, final ConsistencyLevel cLevel) throws Exception {
        IOperation operation = new IOperation() {
            @Override
            public Object execute(Connection conn) throws Exception {
                return conn.getAPI().get_count(keyspace, rowKey, colParent, cLevel);
            }
        };
        return (Integer) tryOperation(operation);
    }

    /**
     * Retrieve a column from a row.
     * @param rowKey                        The key of the row
     * @param columnFamily                  The name of the column family containing the column
     * @param colName                       The name of the column to retrieve
     * @param cLevel                        The Cassandra consistency level with which to perform the operation
     * @return                              The requested <code>Column</code>
     * @throws Exception
     */
    public Column getColumnFromRow(final String rowKey, final String columnFamily, final String colName, final ConsistencyLevel cLevel) throws Exception {
        return getColumnFromRow(rowKey, columnFamily, toBytes(colName), cLevel);
    }

    /**
     * Retrieve a column from a row.
     * @param rowKey                        The key of the row
     * @param columnFamily                  The name of the column family containing the column
     * @param colName                       The name of the column to retrieve
     * @param cLevel                        The Cassandra consistency level with which to perform the operation
     * @return                              The requested <code>Column</code>
     * @throws Exception
     */
    public Column getColumnFromRow(final String rowKey, final String columnFamily, final byte[] colName, final ConsistencyLevel cLevel) throws Exception {
        IOperation operation = new IOperation() {
            @Override
            public Object execute(Connection conn) throws Exception {
                ColumnPath cp = new ColumnPath(columnFamily);
                cp.setColumn(colName);
                ColumnOrSuperColumn cosc = conn.getAPI().get(keyspace, rowKey, cp, cLevel);
                return cosc.column;
            }
        };
        return (Column) tryOperation(operation);
    }

    /**
     * Retrieve a super column from a row.
     * @param rowKey                        The key of the row
     * @param columnFamily                  The name of the column family containing the super column
     * @param colName                       The name of the super column to retrieve
     * @param cLevel                        The Cassandra consistency level with which to perform the operation
     * @return                              The requested <code>SuperColumn</code>
     * @throws Exception
     */
    public SuperColumn getSuperColumnFromRow(final String rowKey, final String columnFamily, final String superColName, final ConsistencyLevel cLevel) throws Exception {
        return getSuperColumnFromRow(rowKey, columnFamily, toBytes(superColName), cLevel);
    }

    /**
     * Retrieve a super column from a row.
     * @param rowKey                        The key of the row
     * @param columnFamily                  The name of the column family containing the super column
     * @param superColName                  The name of the super column to retrieve
     * @param cLevel                        The Cassandra consistency level with which to perform the operation
     * @return                              The requested <code>SuperColumn</code>
     * @throws Exception
     */
    public SuperColumn getSuperColumnFromRow(final String rowKey, final String columnFamily, final byte[] superColName, final ConsistencyLevel cLevel) throws Exception {
        IOperation operation = new IOperation() {
            @Override
            public Object execute(Connection conn) throws Exception {
                ColumnPath cp = new ColumnPath(columnFamily);
                cp.setSuper_column(superColName);
                ColumnOrSuperColumn cosc = conn.getAPI().get(keyspace, rowKey, cp, cLevel);
                return cosc.super_column;
            }
        };
        return (SuperColumn) tryOperation(operation);
    }

    /**
     * Retrieve a sub column from a super column in a row.
     * @param rowKey                        The key of the row
     * @param columnFamily                  The name of the column family containing the super column
     * @param superColName                  The name of the super column containing the sub column
     * @param subColName                    The name of the sub column to retrieve
     * @param cLevel                        The Cassandra consistency level with which to perform the operation
     * @return                              The requested <code>Column</code>
     * @throws Exception
     */
    public Column getSubColumnFromRow(final String rowKey, final String columnFamily, final byte[] superColName, final String subColName, final ConsistencyLevel cLevel) throws Exception {
        return getSubColumnFromRow(rowKey, columnFamily, superColName, toBytes(subColName), cLevel);
    }

    /**
     * Retrieve a sub column from a super column in a row.
     * @param rowKey                        The key of the row
     * @param columnFamily                  The name of the column family containing the super column
     * @param superColName                  The name of the super column containing the sub column
     * @param subColName                    The name of the sub column to retrieve
     * @param cLevel                        The Cassandra consistency level with which to perform the operation
     * @return                              The requested <code>Column</code>
     * @throws Exception
     */
    public Column getSubColumnFromRow(final String rowKey, final String columnFamily, final String superColName, final String subColName, final ConsistencyLevel cLevel) throws Exception {
        return getSubColumnFromRow(rowKey, columnFamily, toBytes(superColName), toBytes(subColName), cLevel);
    }

    /**
     * Retrieve a sub column from a super column in a row.
     * @param rowKey                        The key of the row
     * @param columnFamily                  The name of the column family containing the super column
     * @param superColName                  The name of the super column containing the sub column
     * @param subColName                    The name of the sub column to retrieve
     * @param cLevel                        The Cassandra consistency level with which to perform the operation
     * @return                              The requested <code>Column</code>
     * @throws Exception
     */
    public Column getSubColumnFromRow(final String rowKey, final String columnFamily, final String superColName, final byte[] subColName, final ConsistencyLevel cLevel) throws Exception {
        return getSubColumnFromRow(rowKey, columnFamily, toBytes(superColName), subColName, cLevel);
    }

    /**
     * Retrieve a sub column from a super column in a row.
     * @param rowKey                        The key of the row
     * @param columnFamily                  The name of the column family containing the super column
     * @param superColName                  The name of the super column containing the sub column
     * @param subColName                    The name of the sub column to retrieve
     * @param cLevel                        The Cassandra consistency level with which to perform the operation
     * @return                              The requested <code>Column</code>
     * @throws Exception
     */
    public Column getSubColumnFromRow(final String rowKey, final String columnFamily, final byte[] superColName, final byte[] subColName, final ConsistencyLevel cLevel) throws Exception {
        IOperation operation = new IOperation() {
            @Override
            public Object execute(Connection conn) throws Exception {
                ColumnPath cp = new ColumnPath(columnFamily);
                cp.setSuper_column(superColName);
                cp.setColumn(subColName);
                ColumnOrSuperColumn cosc = conn.getAPI().get(keyspace, rowKey, cp, cLevel);
                return cosc.column;
            }
        };
        return (Column) tryOperation(operation);
    }

    /**
     * Retrieve columns from a row.
     * @param rowKey                        The key of the row
     * @param columnFamily                  The name of the column family containing the columns
     * @param colPredicate                  The column selector predicate
     * @param cLevel                        The Cassandra consistency level with which to perform the operation
     * @return                              A list of matching columns
     * @throws Exception
     */
    public List<Column> getColumnsFromRow(String rowKey, String columnFamily, SlicePredicate colPredicate, ConsistencyLevel cLevel) throws Exception {

        return getColumnsFromRow(rowKey, newColumnParent(columnFamily), colPredicate, cLevel);
    }

    /**
     * Retrieve sub-columns from a super column in a row.
     * @param rowKey                        The key of the row
     * @param columnFamily                  The name of the column family containing the super columns
     * @param superColName                  The name of the super column
     * @param colPredicate                  The sub-column selector predicate
     * @param cLevel                        The Cassandra consistency level with which to perform the operation
     * @return                              A list of matching columns
     * @throws Exception
     */
    public List<Column> getSubColumnsFromRow(String rowKey, String columnFamily, byte[] superColName, SlicePredicate colPredicate, ConsistencyLevel cLevel) throws Exception {

        return getColumnsFromRow(rowKey, newColumnParent(columnFamily, superColName), colPredicate, cLevel);
    }

    /**
     * Retrieve sub-columns from a super column in a row.
     * @param rowKey                        The key of the row
     * @param columnFamily                  The name of the column family containing the super columns
     * @param superColName                  The name of the super column
     * @param colPredicate                  The sub-column selector predicate
     * @param cLevel                        The Cassandra consistency level with which to perform the operation
     * @return                              A list of matching columns
     * @throws Exception
     */
    public List<Column> getSubColumnsFromRow(String rowKey, String columnFamily, String superColName, SlicePredicate colPredicate, ConsistencyLevel cLevel) throws Exception {

        return getColumnsFromRow(rowKey, newColumnParent(columnFamily, superColName), colPredicate, cLevel);
    }

    @SuppressWarnings("unchecked")
    private List<Column> getColumnsFromRow(final String rowKey, final ColumnParent colParent, final SlicePredicate colPredicate, final ConsistencyLevel cLevel) throws Exception {
        IOperation operation = new IOperation() {
            @Override
            public Object execute(Connection conn) throws Exception {
                List<ColumnOrSuperColumn> apiResult = conn.getAPI().get_slice(keyspace, rowKey, colParent, colPredicate, cLevel);
                List<Column> result = new ArrayList<Column>(apiResult.size());
                for (ColumnOrSuperColumn cosc : apiResult)
                    result.add(cosc.column);
                return result;
            }
        };
        return (List<Column>) tryOperation(operation);
    }

    /**
     * Retrieve super columns from a row.
     * @param rowKey                        The key of the row
     * @param columnFamily                  The name of the column family containing the super columns
     * @param colPredicate                  The super column selector predicate
     * @param cLevel                        The Cassandra consistency level with which to perform the operation
     * @return                              A list of matching columns
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public List<SuperColumn> getSuperColumnsFromRow(final String rowKey, final String columnFamily, final SlicePredicate colPredicate, final ConsistencyLevel cLevel) throws Exception {
        IOperation operation = new IOperation() {
            @Override
            public Object execute(Connection conn) throws Exception {
                List<ColumnOrSuperColumn> apiResult = conn.getAPI().get_slice(keyspace, rowKey, newColumnParent(columnFamily), colPredicate, cLevel);
                List<SuperColumn> result = new ArrayList<SuperColumn>(apiResult.size());
                for (ColumnOrSuperColumn cosc : apiResult)
                    result.add(cosc.super_column);
                return result;
            }
        };
        return (List<SuperColumn>) tryOperation(operation);
    }

    public enum OrderType {
        BytesType,
        AsciiType,
        UTF8Type,
        LongType,
        LexicalUUIDType,
        TimeUUIDType
    }

    /**
     * Retrieve a page of columns composed from a segment of the sequence of columns in a row.
     * @param rowKey                        The key of the row
     * @param columnFamily                  The name of the column family containing the columns
     * @param startBeyondName               The sequence of columns must begin with the smallest column name greater than this value. Pass <code>null</code> to start at the beginning of the sequence.
     * @param orderType                     The scheme used to determine how the column names are ordered
     * @param reversed                      Whether the scan should proceed in descending column name order
     * @param count                         The maximum number of columns that can be retrieved by the scan
     * @param cLevel                        The Cassandra consistency level with which to perform the operation
     * @return                              A page of columns
     * @throws Exception
     */
    public List<Column> getPageOfColumnsFromRow(final String rowKey, final String columnFamily, final byte[] startBeyondName, final OrderType orderType, final boolean reversed, final int count, final ConsistencyLevel cLevel) throws Exception {

        SlicePredicate predicate;
        if (startBeyondName == null)
            predicate = Selector.newColumnsPredicateAll(reversed, count);
        else {
            byte[] startName;
            if (reversed)
                startName = Selector.bumpDownColumnName(startBeyondName, orderType);
            else
                startName = Selector.bumpUpColumnName(startBeyondName, orderType);

            predicate = Selector.newColumnsPredicate(startName, new byte[] {}, reversed, count);
        }
        return getColumnsFromRow(rowKey, columnFamily, predicate, cLevel);
    }

    /**
     * Retrieve a page of super columns composed from a segment of the sequence of super columns in a row.
     * @param rowKey                        The key of the row
     * @param columnFamily                  The name of the column family containing the super columns
     * @param startBeyondName               The sequence of super columns must begin with the smallest super column name greater than this value. Pass <code>null</code> to start at the beginning of the sequence.
     * @param orderType                     The scheme used to determine how the column names are ordered
     * @param reversed                      Whether the scan should proceed in descending super column name order
     * @param count                         The maximum number of super columns that can be retrieved by the scan
     * @param cLevel                        The Cassandra consistency level with which to perform the operation
     * @return                              A page of super columns
     * @throws Exception
     */
    public List<SuperColumn> getPageOfSuperColumnsFromRow(final String rowKey, final String columnFamily, final byte[] startBeyondName, final OrderType orderType, final boolean reversed, final int count, final ConsistencyLevel cLevel) throws Exception {

        SlicePredicate predicate;
        if (startBeyondName == null)
            predicate = Selector.newColumnsPredicateAll(reversed, count);
        else {
            byte[] startName;
            if (reversed)
                startName = Selector.bumpDownColumnName(startBeyondName, orderType);
            else
                startName = Selector.bumpUpColumnName(startBeyondName, orderType);

            predicate = Selector.newColumnsPredicate(startName, new byte[] {}, reversed, count);
        }
        return getSuperColumnsFromRow(rowKey, columnFamily, predicate, cLevel);
    }

    /**
     * Retrieve columns from a set of rows.
     * @param rowKeys                        The keys of the rows
     * @param columnFamily                   The name of the column family containing the columns
     * @param colPredicate                   The column selector predicate
     * @param cLevel                         The Cassandra consistency level with which to perform the operation
     * @return                               A map from row keys to the matching lists of columns
     * @throws Exception
     */
    public Map<String, List<Column>> getColumnsFromRows(List<String> rowKeys, String columnFamily, SlicePredicate colPredicate, ConsistencyLevel cLevel) throws Exception {

        return getColumnsFromRows(rowKeys, newColumnParent(columnFamily), colPredicate, cLevel);
    }

    /**
     * Retrieve sub-columns from a super column in a set of rows.
     * @param rowKeys                        The keys of the rows
     * @param columnFamily                   The name of the column family containing the super columns
     * @param superColName                   The name of the super column
     * @param colPredicate                   The sub-column selector predicate
     * @param cLevel                         The Cassandra consistency level with which to perform the operation
     * @return                               A map from row keys to the matching lists of sub-columns
     * @throws Exception
     */
    public Map<String, List<Column>> getSubColumnsFromRows(List<String> rowKeys, String columnFamily, String superColName, SlicePredicate colPredicate, ConsistencyLevel cLevel) throws Exception {

        return getColumnsFromRows(rowKeys, newColumnParent(columnFamily, superColName), colPredicate, cLevel);
    }

    /**
     * Retrieve sub-columns from a super column in a set of rows.
     * @param rowKeys                        The keys of the rows
     * @param columnFamily                   The name of the column family containing the super columns
     * @param superColName                   The name of the super column
     * @param colPredicate                   The sub-column selector predicate
     * @param cLevel                         The Cassandra consistency level with which to perform the operation
     * @return                               A map from row keys to the matching lists of sub-columns
     * @throws Exception
     */
    public Map<String, List<Column>> getSubColumnsFromRows(List<String> rowKeys, String columnFamily, byte[] superColName, SlicePredicate colPredicate, ConsistencyLevel cLevel) throws Exception {

        return getColumnsFromRows(rowKeys, newColumnParent(columnFamily, superColName), colPredicate, cLevel);
    }

    /**
     * Retrieve super columns from a set of rows.
     * @param rowKeys                        The keys of the rows
     * @param columnFamily                   The name of the column family containing the super columns
     * @param colPredicate                   The super column selector predicate
     * @param cLevel                         The Cassandra consistency level with which to perform the operation
     * @return                               A map from row keys to the matching lists of super columns
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public Map<String, List<SuperColumn>> getSuperColumnsFromRows(final List<String> rowKeys, final String columnFamily, final SlicePredicate colPredicate, final ConsistencyLevel cLevel) throws Exception {
        IOperation operation = new IOperation() {
            @Override
            public Object execute(Connection conn) throws Exception {
                Map<String, List<ColumnOrSuperColumn>> apiResult = conn.getAPI().multiget_slice(keyspace, rowKeys, newColumnParent(columnFamily), colPredicate, cLevel);
                Map<String, List<SuperColumn>> result = new HashMap<String, List<SuperColumn>>();
                for (String rowKey : apiResult.keySet()) {
                    List<ColumnOrSuperColumn> coscList = apiResult.get(rowKey);
                    List<SuperColumn> columns = new ArrayList<SuperColumn>(coscList.size());
                    for (ColumnOrSuperColumn cosc : coscList)
                        columns.add(cosc.super_column);
                    result.put(rowKey, columns);
                }
                return result;
            }
        };
        return (Map<String, List<SuperColumn>>) tryOperation(operation);
    }

    @SuppressWarnings("unchecked")
    private Map<String, List<Column>> getColumnsFromRows(final List<String> rowKeys, final ColumnParent colParent, final SlicePredicate colPredicate, final ConsistencyLevel cLevel) throws Exception {
        IOperation operation = new IOperation() {
            @Override
            public Object execute(Connection conn) throws Exception {
                Map<String, List<ColumnOrSuperColumn>> apiResult = conn.getAPI().multiget_slice(keyspace, rowKeys, colParent, colPredicate, cLevel);
                Map<String, List<Column>> result = new HashMap<String, List<Column>>();
                for (String rowKey : apiResult.keySet()) {
                    List<ColumnOrSuperColumn> coscList = apiResult.get(rowKey);
                    List<Column> columns = new ArrayList<Column>(coscList.size());
                    for (ColumnOrSuperColumn cosc : coscList)
                        columns.add(cosc.column);
                    result.put(rowKey, columns);
                }
                return result;
            }
        };
        return (Map<String, List<Column>>) tryOperation(operation);
    }

    /**
     * Retrieve columns from a range of rows.
     * @param keyRange                        A key range selecting the rows
     * @param columnFamily                    The name of the column family containing the columns
     * @param colPredicate                    The column selector predicate
     * @param cLevel                          The Cassandra consistency level with which to perform the operation
     * @return                                A map from row keys to the matching lists of columns
     * @throws Exception
     */
    public SortedMap<String, List<Column>> getColumnsFromRows(KeyRange keyRange, String columnFamily, SlicePredicate colPredicate, ConsistencyLevel cLevel) throws Exception {

        return getColumnsFromRows(keyRange, newColumnParent(columnFamily), colPredicate, cLevel);
    }

    /**
     * Retrieve sub-columns from a super column in a range of rows.
     * @param keyRange                        A key range selecting the rows
     * @param columnFamily                    The name of the column family containing the super columns
     * @param superColName                    The name of the super column
     * @param colPredicate                    The sub-column selector predicate
     * @param cLevel                          The Cassandra consistency level with which to perform the operation
     * @return                                A map from row keys to the matching lists of sub-columns
     * @throws Exception
     */
    public SortedMap<String, List<Column>> getSubColumnsFromRows(KeyRange keyRange, String columnFamily, byte[] superColName, SlicePredicate colPredicate, ConsistencyLevel cLevel) throws Exception {

        return getColumnsFromRows(keyRange, newColumnParent(columnFamily, superColName), colPredicate, cLevel);
    }

    /**
     * Retrieve sub-columns from a super column in a range of rows.
     * @param keyRange                        A key range selecting the rows
     * @param columnFamily                    The name of the column family containing the super columns
     * @param superColName                    The name of the super column
     * @param colPredicate                    The sub-column selector predicate
     * @param cLevel                          The Cassandra consistency level with which to perform the operation
     * @return                                A map from row keys to the matching lists of sub-columns
     * @throws Exception
     */
    public SortedMap<String, List<Column>> getSubColumnsFromRows(KeyRange keyRange, String columnFamily, String superColName, SlicePredicate colPredicate, ConsistencyLevel cLevel) throws Exception {

        return getColumnsFromRows(keyRange, newColumnParent(columnFamily, superColName), colPredicate, cLevel);
    }

    /**
     * Retrieve super columns from a range of rows.
     * @param keyRange                        A key range selecting the rows
     * @param columnFamily                    The name of the column family containing the super columns
     * @param colPredicate                    The super column selector predicate
     * @param cLevel                          The Cassandra consistency level with which to perform the operation
     * @return                                A map from row keys to the matching lists of super columns
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public SortedMap<String, List<SuperColumn>> getSuperColumnsFromRows(final KeyRange keyRange, final String columnFamily, final SlicePredicate colPredicate, final ConsistencyLevel cLevel) throws Exception {
        IOperation operation = new IOperation() {
            @Override
            public Object execute(Connection conn) throws Exception {
                List<KeySlice> apiResult = conn.getAPI().get_range_slices(keyspace, newColumnParent(columnFamily), colPredicate, keyRange, cLevel);
                SortedMap<String, List<SuperColumn>> result = new TreeMap<String, List<SuperColumn>>();
                for (KeySlice ks : apiResult) {
                    List<ColumnOrSuperColumn> coscList = ks.columns;
                    List<SuperColumn> colList = new ArrayList<SuperColumn>(coscList.size());
                    for (ColumnOrSuperColumn cosc : coscList)
                        colList.add(cosc.super_column);
                    result.put(ks.key, colList);
                }
                return result;
            }
        };
        return (SortedMap<String, List<SuperColumn>>) tryOperation(operation);
    }

    @SuppressWarnings("unchecked")
    private SortedMap<String, List<Column>> getColumnsFromRows(final KeyRange keyRange, final ColumnParent colParent, final SlicePredicate colPredicate, final ConsistencyLevel cLevel) throws Exception {
        IOperation operation = new IOperation() {
            @Override
            public Object execute(Connection conn) throws Exception {
                List<KeySlice> apiResult = conn.getAPI().get_range_slices(keyspace, colParent, colPredicate, keyRange, cLevel);
                SortedMap<String, List<Column>> result = new TreeMap<String, List<Column>>();
                for (KeySlice ks : apiResult) {
                    List<ColumnOrSuperColumn> coscList = ks.columns;
                    List<Column> colList = new ArrayList<Column>(coscList.size());
                    for (ColumnOrSuperColumn cosc : coscList)
                        colList.add(cosc.column);
                    result.put(ks.key, colList);
                }
                return result;
            }
        };
        return (SortedMap<String, List<Column>>) tryOperation(operation);
    }

    /**
     * Create a new <code>SlicePredicate</code> instance that selects "all" columns
     * @param reversed                        Whether the results should be returned in reverse order
     * @param maxColCount                     The maximum number of columns to return
     * @return                                The new <code>SlicePredicate</code>
     */
    public static SlicePredicate newColumnsPredicateAll(boolean reversed, int maxColCount) {
        SlicePredicate predicate = new SlicePredicate();
        predicate.setSlice_range(new SliceRange(new byte[] {}, new byte[] {}, reversed, maxColCount));
        return predicate;
    }

    /**
     * Create a new <code>SlicePredicate</code> instance.
     * @param startName                       The inclusive column start name of the range to select in the slice
     * @param finishName                      The inclusive column end name of the range to select in the slice
     * @param reversed                        Whether the results should be returned in reverse order
     * @param maxColCount                     The maximum number of columns to return
     * @return                                The new <code>SlicePredicate</code>
     */
    public static SlicePredicate newColumnsPredicate(byte[] startName, byte[] finishName, boolean reversed, int maxColCount) {
        SlicePredicate predicate = new SlicePredicate();
        predicate.setSlice_range(new SliceRange(startName, finishName, reversed, maxColCount));
        return predicate;
    }

    /**
     * Create a new <code>SlicePredicate</code> instance.
     * @param startName                       The inclusive column start name of the range to select in the slice
     * @param finishName                      The inclusive column end name of the range to select in the slice
     * @param reversed                        Whether the results should be returned in reverse order
     * @param maxColCount                     The maximum number of columns to return
     * @return                                The new <code>SlicePredicate</code>
     */
    public static SlicePredicate newColumnsPredicate(String startName, String finishName, boolean reversed, int maxColCount) {
        SlicePredicate predicate = new SlicePredicate();
        predicate.setSlice_range(new SliceRange(toBytes(startName), toBytes(finishName), reversed, maxColCount));
        return predicate;
    }

    /**
     * Create a new <code>SlicePredicate</code> instance.
     * @param colNames                        The specific columns names to select in the slice
     * @return                                The new <code>SlicePredicate</code>
     */
    public static SlicePredicate newColumnsPredicate(String... colNames) {
        List<byte[]> asList = new ArrayList<byte[]>(32);
        for (String colName : colNames)
            asList.add(toBytes(colName));
        SlicePredicate predicate = new SlicePredicate();
        predicate.setColumn_names(asList);
        return predicate;
    }

    /**
     * Create a new <code>SlicePredicate</code> instance.
     * @param colNames                        The specific columns names to select in the slice
     * @return                                The new <code>SlicePredicate</code>
     */
    public static SlicePredicate newColumnsPredicate(byte[]... colNames) {
        List<byte[]> asList = new ArrayList<byte[]>(32);
        for (byte[] colName : colNames)
            asList.add(colName);
        SlicePredicate predicate = new SlicePredicate();
        predicate.setColumn_names(asList);
        return predicate;
    }

    /**
     * Create a new <code>KeyRange</code> instance.
     * @param startKey                        The inclusive start key of the range
     * @param finishKey                       The inclusive finish key of the range
     * @param maxKeyCount                     The maximum number of keys to be scanned
     * @return                                The new <code>KeyRange</code> instance
     */
    public static KeyRange newKeyRange(String startKey, String finishKey, int maxKeyCount) {
        KeyRange keyRange = new KeyRange(maxKeyCount);
        keyRange.setStart_key(startKey);
        keyRange.setEnd_key(finishKey);
        return keyRange;
    }

    /**
     * Create a new <code>KeyRange</code> instance.
     * @param startFollowingKey                The exclusive start key of the ring range
     * @param finishKey                        The inclusive finish key of the range (can be less than <code>startFollowing</code>)
     * @param maxKeyCount                      The maximum number of keys to be scanned
     * @return                                 The new <code>KeyRange</code> instance
     */
    public static KeyRange newKeyRingRange(String startFollowingKey, String finishKey, int maxKeyCount) {
        KeyRange keyRange = new KeyRange(maxKeyCount);
        keyRange.setStart_token(startFollowingKey);
        keyRange.setEnd_token(finishKey);
        return keyRange;
    }

    /**
     * Determines if a super column with a particular name exist in the list of super columns.
     * @param superColumns                    The list of super columns
     * @param superColName                    The name of the super column
     * @return                                Whether the super column is present
     */
    public static boolean superColumnExists(List<SuperColumn> superColumns, String superColName) {
        return superColumnExists(superColumns, toBytes(superColName));
    }

    /**
     * Determines if a super column with a particular name exist in the list of super columns.
     * @param superColumns                    The list of super columns
     * @param superColName                    The name of the super column
     * @return                                Whether the super column is present
     */
    public static boolean superColumnExists(List<SuperColumn> superColumns, byte[] superColName) {
        for (SuperColumn superColumn : superColumns)
            if (Arrays.equals(superColumn.name, superColName))
                return true;
        return false;
    }

    /**
     * Get a super column by name from a list of super columns
     * @param superColumns                    The list of super columns
     * @param superColName                    The name of the super column
     * @return                                The super column
     * @throws ArrayIndexOutOfBoundsException    Thrown if the list does not contain a super column with the specified name
     */
    public static SuperColumn getSuperColumn(List<SuperColumn> superColumns, byte[] superColName) throws ArrayIndexOutOfBoundsException {
        for (SuperColumn superColumn : superColumns)
            if (Arrays.equals(superColumn.name, superColName))
                return superColumn;
        throw new ArrayIndexOutOfBoundsException();
    }

    /**
     * Get a super column by name from a list of super columns
     * @param superColumns                    The list of super columns
     * @param superColName                    The name of the super column
     * @return                                The super column
     * @throws ArrayIndexOutOfBoundsException    Thrown if the list does not contain a super column with the specified name
     */
    public static SuperColumn getSuperColumn(List<SuperColumn> superColumns, String superColName) throws ArrayIndexOutOfBoundsException {
        return getSuperColumn(superColumns, toBytes(superColName));
    }

    /**
     * Get the value of a column in a list of columns
     * @param columns                         The list of columns
     * @param colName                         The name of the column from which to retrieve the value
     * @param defaultValue                    A default value to return if a column with the specified name is not present in the list
     * @return                                The column value
     * @throws UnsupportedEncodingException    Thrown if the column value was not a string
     */
    public static String getColumnValue(List<Column> columns, String colName, String defaultValue) throws UnsupportedEncodingException {
        return getColumnValue(columns, toBytes(colName), defaultValue);
    }

    /**
     * Get the value of a column in a list of columns
     * @param columns                         The list of columns
     * @param colName                         The name of the column from which to retrieve the value
     * @param defaultValue                    A default value to return if a column with the specified name is not present in the list
     * @return                                The column value
     */
    public static byte[] getColumnValue(List<Column> columns, String colName, byte[] defaultValue) {
        return getColumnValue(columns, toBytes(colName), defaultValue);
    }

    /**
     * Get the value of a column in a list of columns
     * @param columns                        The list of columns
     * @param colName                        The name of the column from which to retrieve the value
     * @param defaultValue                   A default value to return if a column with the specified name is not present in the list
     * @return                                The column value
     */
    public static byte[] getColumnValue(List<Column> columns, byte[] colName, byte[] defaultValue) {
        for (Column column : columns)
            if (Arrays.equals(column.name, colName))
                return column.value;
        return defaultValue;
    }

    /**
     * Get the value of a column in a list of columns
     * @param columns                        The list of columns
     * @param colName                        The name of the column from which to retrieve the value
     * @param defaultValue                   A default value to return if a column with the specified name is not present in the list
     * @return                               The column value
     * @throws UnsupportedEncodingException    Thrown if the column value was not a string
     */
    public static String getColumnValue(List<Column> columns, byte[] colName, String defaultValue) throws UnsupportedEncodingException {
        for (Column column : columns)
            if (Arrays.equals(column.name, colName))
                return toUTF8(column.value);
        return defaultValue;
    }

    /**
     * Determines if a column with a particular name exist in the list of columns.
     * @param superColumns                    The list of columns
     * @param superColName                    The name of the column
     * @return                                Whether the column is present
     */
    public static boolean columnExists(List<Column> columns, byte[] colName) {
        for (Column column : columns)
            if (Arrays.equals(column.name, colName))
                return true;
        return false;
    }

    /**
     * Determines if a column with a particular name exist in the list of columns.
     * @param superColumns                    The list of columns
     * @param superColName                    The name of the column
     * @return                                Whether the column is present
     */
    public static boolean columnExists(List<Column> columns, String colName) {
        return columnExists(columns, toBytes(colName));
    }

    /**
     * Get the value of a column in a list of columns
     * @param columns                        The list of columns
     * @param colName                        The name of the column from which to retrieve the value
     * @return                               The column value
     * @throws ArrayIndexOutOfBoundsException    Thrown if the specified column was not found
     */
    public static byte[] getColumnValue(List<Column> columns, byte[] colName) throws ArrayIndexOutOfBoundsException {
        for (Column column : columns)
            if (Arrays.equals(column.name, colName))
                return column.value;
        throw new ArrayIndexOutOfBoundsException();
    }

    /**
     * Get the value of a column in a list of columns
     * @param columns                        The list of columns
     * @param colName                        The name of the column from which to retrieve the value
     * @return                               The column value
     * @throws ArrayIndexOutOfBoundsException    Thrown if the specified column was not found
     */
    public static byte[] getColumnValue(List<Column> columns, String colName) throws ArrayIndexOutOfBoundsException {
        return getColumnValue(columns, toBytes(colName));
    }

    /**
     * Get the value of a column in a list of columns
     * @param columns                        The list of columns
     * @param colName                        The name of the column from which to retrieve the value
     * @return                               The column value as a <code>String</code>
     * @throws ArrayIndexOutOfBoundsException    Thrown if the specified column was not found
     * @throws UnsupportedEncodingException     Thrown if the column value did not contain a valid UTF-8 string
     */
    public static String getColumnStringValue(List<Column> columns, String colName) throws ArrayIndexOutOfBoundsException, UnsupportedEncodingException {
        return getColumnStringValue(columns, toBytes(colName));
    }

    /**
     * Get the value of a column in a list of columns
     * @param columns                        The list of columns
     * @param colName                        The name of the column from which to retrieve the value
     * @return                               The column value as a <code>String</code>
     * @throws ArrayIndexOutOfBoundsException    Thrown if the specified column was not found
     * @throws UnsupportedEncodingException     Thrown if the column value did not contain a valid UTF-8 string
     */
    public static String getColumnStringValue(List<Column> columns, byte[] colName) throws ArrayIndexOutOfBoundsException, UnsupportedEncodingException {
        for (Column column : columns)
            if (Arrays.equals(column.name, colName))
                return toUTF8(column.value);
        throw new ArrayIndexOutOfBoundsException();
    }

    /**
     * Get the time stamp of a column in a list of columns.
     * @param columns                        The list of columns
     * @param colName                        The name of the column from which to retrieve the timestamp
     * @return                               The time stamp (the <code>Mutator</code> object uses time stamps as microseconds)
     * @throws ArrayIndexOutOfBoundsException
     */
    public static long getColumnTimestamp(List<Column> columns, byte[] colName) throws ArrayIndexOutOfBoundsException {
        for (Column column : columns)
            if (Arrays.equals(column.name, colName))
                return column.timestamp;
        throw new ArrayIndexOutOfBoundsException();
    }

    /**
     * Get the time stamp of a column in a list of columns.
     * @param columns                        The list of columns
     * @param colName                        The name of the column from which to retrieve the timestamp
     * @return                               The time stamp (the <code>Mutator</code> object uses time stamps as microseconds)
     * @throws ArrayIndexOutOfBoundsException
     */
    public static long getColumnTimestamp(List<Column> columns, String colName) throws ArrayIndexOutOfBoundsException {
        return getColumnTimestamp(columns, toBytes(colName));
    }

    /**
     * Return a bumped up copy of the column name that it is the minimum value greater than the specified value.
     * <code>null</code> is returned if no such value exists.
     * @param colName                        The column name
     * @return                               The bumped up column name, or same value if there is no higher value
     * @throws UnsupportedEncodingException
     */
    public static byte[] bumpUpColumnName(String colName, OrderType orderType) {
        if (orderType != OrderType.UTF8Type)
            throw new IllegalArgumentException("You must pass a UTF-8 column name to this function variant");
        StringBuilder sb = new StringBuilder(32);
        boolean doneInc = false;
        for (int i=colName.length()-1; i >= 0; i--) {
            char c = colName.charAt(i);
            if (!doneInc && c < Character.MAX_VALUE) {
                sb.append(++c);
                doneInc = true;
            } else {
                sb.append(c);
            }
        }
        sb.reverse();
        return toBytes(sb.toString());
    }

    /**
     * Return a bumped up copy of the column name that it is the minimum value greater than the specified value.
     * <code>null</code> is returned if no such value exists.
     * @param colName                        The column name
     * @return                               The bumped up column name, or same value if there is no higher value
     * @throws UnsupportedEncodingException
     */
    public static byte[] bumpUpColumnName(byte[] colName, OrderType orderType) {

        if (orderType == OrderType.BytesType) {
            byte[] newName = Arrays.copyOf(colName, colName.length);
            for (int i=newName.length-1; i >= 0; i--) {
                if ((newName[i] & 0xFF) < 255) {
                    newName[i] = (byte)((newName[i] & 0xFF) + 1);
                    break;
                }
            }
            return newName;
        }
        else if (orderType == OrderType.LongType) {
            if (colName.length != 8)
                throw new IllegalArgumentException("A Long argument should be 8 bytes");
            long l = NumberHelper.toLong(colName);
            if (l < Long.MAX_VALUE)
                l++;
            return NumberHelper.toBytes(l);
        }
        else if (orderType == OrderType.LexicalUUIDType) {
            if (colName.length != 16)
                throw new IllegalArgumentException("A lexical UUID argument should be 16 bytes");
            long msb = NumberHelper.toLong(colName, 0, 8);
            long lsb = NumberHelper.toLong(colName, 8, 8);
            if (lsb < Long.MAX_VALUE)
                lsb++;
            else
                if (msb < Long.MAX_VALUE)
                    msb++;
            byte[] newName = new byte[16];
            NumberHelper.toBytes(msb, newName, 0, 8);
            NumberHelper.toBytes(lsb, newName, 8, 8);
            return newName;
        }
        else if (orderType == OrderType.TimeUUIDType) {
            if (colName.length != 16)
                throw new IllegalArgumentException("A time UUID argument should be 16 bytes");
            byte[] newName = Arrays.copyOf(colName, 16);
            for (int i=15; i >= 8; i--) {
                if ((newName[i] & 0xFF) < 255) {
                    newName[i] = (byte)((newName[i] & 0xFF) + 1);
                    return newName;
                }
            }
            long timestamp = NumberHelper.toLong(newName, 0, 8);
            if (timestamp < Long.MAX_VALUE)
                timestamp++;
            NumberHelper.toBytes(timestamp, newName, 0, 8);
            return newName;
        }
        throw new UnsupportedOperationException("Not implemented yet. Please update Pelops.");
    }

    /**
     * Return a bumped down copy of the column name that is the maximum value less than the specified value.
     * <code>null</code> is returned if no such value exists.
     * @param colName                        The column name
     * @return                               The bumped down column name, or same value if there is no lower value
     */
    public static byte[] bumpDownColumnName(String colName, OrderType orderType) {
        if (orderType != OrderType.UTF8Type)
            throw new IllegalArgumentException("You must pass a UTF-8 column name to this function variant");
        StringBuilder sb = new StringBuilder(32);
        boolean doneDec = false;
        for (int i=colName.length()-1; i >= 0; i--) {
            char c = colName.charAt(i);
            if (!doneDec && c > Character.MIN_VALUE) {
                sb.append(--c);
                doneDec = true;
            } else {
                sb.append(c);
            }
        }
        sb.reverse();
        return toBytes(sb.toString());
    }

    /**
     * Return a bumped down copy of the column name that is the maximum value less than the specified value.
     * <code>null</code> is returned if no such value exists.
     * @param colName                        The column name
     * @return                               The bumped down column name, or same value if there is no lower value
     */
    public static byte[] bumpDownColumnName(byte[] colName, OrderType orderType) {

        if (orderType == OrderType.BytesType) {
            byte[] newName = Arrays.copyOf(colName, colName.length);
            for (int i=newName.length-1; i >= 0; i--) {
                if ((newName[i] & 0xFF) > 0) {
                    newName[i] = (byte)((newName[i] & 0xFF) - 1);
                    break;
                }
            }
            return newName;
        }
        else if (orderType == OrderType.LongType) {
            if (colName.length != 8)
                throw new IllegalArgumentException("A Long argument should be 8 bytes");
            long l = NumberHelper.toLong(colName);
            if (l > Long.MIN_VALUE)
                l--;
            return NumberHelper.toBytes(l);
        }
        else if (orderType == OrderType.LexicalUUIDType) {
            if (colName.length != 16)
                throw new IllegalArgumentException("A lexical UUID argument should be 16 bytes");
            long msb = NumberHelper.toLong(colName, 0, 8);
            long lsb = NumberHelper.toLong(colName, 8, 8);
            if (lsb > Long.MIN_VALUE)
                lsb--;
            else
                if (msb > Long.MIN_VALUE)
                    msb--;
            byte[] newName = new byte[16];
            NumberHelper.toBytes(msb, newName, 0, 8);
            NumberHelper.toBytes(lsb, newName, 8, 8);
            return newName;
        }
        else if (orderType == OrderType.TimeUUIDType) {
            if (colName.length != 16)
                throw new IllegalArgumentException("A time UUID argument should be 16 bytes");
            byte[] newName = Arrays.copyOf(colName, 16);
            for (int i=15; i >= 8; i--) {
                if ((newName[i] & 0xFF) > 0) {
                    newName[i] = (byte)((newName[i] & 0xFF) - 1);
                    return newName;
                }
            }
            long timestamp = NumberHelper.toLong(newName, 0, 8);
            if (timestamp > Long.MIN_VALUE)
                timestamp--;
            NumberHelper.toBytes(timestamp, newName, 0, 8);
            return newName;
        }
        throw new UnsupportedOperationException("Not implemented yet. Please update Pelops.");
    }

    /**
     * Create a batch mutation operation.
     * @param keyspace                    The keyspace the batch mutation will be executed against.
     */
    protected Selector(ThriftPool thrift, String keyspace) {
        super(thrift, keyspace);
    }

    private static ColumnParent newColumnParent(String columnFamily, String superColName) {
        return newColumnParent(columnFamily, toBytes(superColName));
    }

    private static ColumnParent newColumnParent(String columnFamily, byte[] superColName) {
        ColumnParent parent = new ColumnParent(columnFamily);
        parent.setSuper_column(superColName);
        return parent;
    }

    private static ColumnParent newColumnParent(String columnFamily) {
        return new ColumnParent(columnFamily);
    }
}
