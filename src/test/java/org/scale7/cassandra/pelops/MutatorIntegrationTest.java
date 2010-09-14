package org.scale7.cassandra.pelops;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static org.scale7.cassandra.pelops.Bytes.fromBytes;
import static org.scale7.cassandra.pelops.ColumnFamilyManager.*;
import static org.scale7.cassandra.pelops.Selector.newColumnsPredicateAll;

/**
 * Tests the {@link Selector} class.
 */
public class MutatorIntegrationTest {
    private static final Logger logger = SystemProxy.getLoggerFromFactory(MutatorIntegrationTest.class);

    public static final String CF = "MUT_CF";
    public static final String SCF = "MUT_SCF";

    private static IntegrationTestHelper helper = new IntegrationTestHelper();

    @BeforeClass
    public static void setup() throws Exception {
        helper.setup(Arrays.asList(
                new CfDef(IntegrationTestHelper.KEYSPACE, CF)
                        .setColumn_type(CFDEF_TYPE_STANDARD)
                        .setComparator_type(CFDEF_COMPARATOR_BYTES),
                new CfDef(IntegrationTestHelper.KEYSPACE, SCF)
                        .setColumn_type(CFDEF_TYPE_SUPER)
                        .setComparator_type(CFDEF_COMPARATOR_BYTES)
                        .setSubcomparator_type(CFDEF_COMPARATOR_BYTES)
        ));
    }

    @AfterClass
    public static void teardown() {
        helper.teardown();
    }

    @Before
    public void truncate() throws Exception {
        helper.truncate();
    }

    /**
     * Tests that a write replaces previous values and that the appropriate columns are deleted when null values are set
     * on the columns.
     */
    @Test
    public void testWriteColumnsWithDeletesReplacingPrevious() throws Exception {
        Bytes rowKey = Bytes.fromLong(Long.MAX_VALUE);

        // write out the value to be replaced (deleted)
        Mutator mutator = helper.getPool().createMutator();
        List<Column> columns = mutator.newColumnList(
                mutator.newColumn(Bytes.fromInt(1), Bytes.fromChar('a')),
                mutator.newColumn(Bytes.fromInt(2), Bytes.fromChar('b')),
                mutator.newColumn(Bytes.fromInt(3), Bytes.fromChar('c'))
        );
        mutator.writeColumns(CF, rowKey, columns);
        mutator.execute(ConsistencyLevel.ONE);

        // make sure the data was written as expected
        Selector selector = helper.getPool().createSelector();
        List<Column> persistedColumns = selector.getColumnsFromRow(
                CF, rowKey, newColumnsPredicateAll(false, Integer.MAX_VALUE), ConsistencyLevel.ONE
        );

        verifyColumns(columns, persistedColumns);

        // write out the replacement values
        mutator = helper.getPool().createMutator();
        columns = mutator.newColumnList(
                mutator.newColumn(Bytes.fromInt(1), Bytes.fromChar('d')),
                mutator.newColumn(Bytes.fromInt(2), (Bytes) null),
                mutator.newColumn(Bytes.fromInt(3), new Bytes(null))
        );
        mutator.writeColumns(CF, rowKey, columns, true);
        mutator.execute(ConsistencyLevel.ONE);

        // make sure the data was written as expected and that the appropriate columns have been deleted
        selector = helper.getPool().createSelector();
        persistedColumns = selector.getColumnsFromRow(
                CF, rowKey, newColumnsPredicateAll(false, Integer.MAX_VALUE), ConsistencyLevel.ONE
        );

        verifyColumns(columns.subList(0, 1), persistedColumns);
    }

    /**
     * Tests that a write replaces previous values and that the appropriate columns are deleted when null values are set
     * on the columns.
     */
    @Test
    public void testWriteSubColumnsWithDeletesReplacingPrevious() throws Exception {
        Bytes rowKey = Bytes.fromLong(Long.MAX_VALUE);
        Bytes columnName = Bytes.fromShort(Short.MAX_VALUE);

        // write out the value to be replaced (deleted)
        Mutator mutator = helper.getPool().createMutator();
        List<Column> columns = mutator.newColumnList(
                mutator.newColumn(Bytes.fromInt(1), Bytes.fromChar('a')),
                mutator.newColumn(Bytes.fromInt(2), Bytes.fromChar('b')),
                mutator.newColumn(Bytes.fromInt(3), Bytes.fromChar('c'))
        );
        mutator.writeSubColumns(SCF, rowKey, columnName, columns);
        mutator.execute(ConsistencyLevel.ONE);

        // make sure the data was written as expected
        Selector selector = helper.getPool().createSelector();
        List<Column> persistedColumns = selector.getSubColumnsFromRow(
                SCF, rowKey, columnName, newColumnsPredicateAll(false, Integer.MAX_VALUE), ConsistencyLevel.ONE
        );

        verifyColumns(columns, persistedColumns);

        // write out the replacement values
        mutator = helper.getPool().createMutator();
        columns = mutator.newColumnList(
                mutator.newColumn(Bytes.fromInt(1), Bytes.fromChar('d')),
                mutator.newColumn(Bytes.fromInt(2), (Bytes) null),
                mutator.newColumn(Bytes.fromInt(3), new Bytes(null))
        );
        mutator.writeSubColumns(SCF, rowKey, columnName, columns, true);
        mutator.execute(ConsistencyLevel.ONE);

        // make sure the data was written as expected and that the appropriate columns have been deleted
        selector = helper.getPool().createSelector();
        persistedColumns = selector.getSubColumnsFromRow(
                SCF, rowKey, columnName, newColumnsPredicateAll(false, Integer.MAX_VALUE), ConsistencyLevel.ONE
        );

        verifyColumns(columns.subList(0, 1), persistedColumns);
    }

    private void verifyColumns(List<Column> expectedColumns, List<Column> actualColumns) {
        assertEquals("Wrong number of columns", expectedColumns.size(), actualColumns.size());
        for (int i = 0; i < expectedColumns.size(); i++) {
            Column expectedColumn = expectedColumns.get(i);
            Column actualColumn = actualColumns.get(i);

            assertEquals("Column names didn't match", fromBytes(expectedColumn.getName()), fromBytes(actualColumn.getName()));
            assertEquals("Column values didn't match", fromBytes(expectedColumn.getValue()), fromBytes(actualColumn.getValue()));
        }
    }
}
