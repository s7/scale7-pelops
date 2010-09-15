package org.scale7.cassandra.pelops;

import org.apache.cassandra.thrift.*;
import org.apache.thrift.protocol.TProtocolException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.*;
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

    @Test
    public void testConstructorDeleteIfNullState() {
        IThriftPool pool = Mockito.mock(IThriftPool.class);
        Mutator mutator = new Mutator(pool, new Clock(System.currentTimeMillis()), true);
        assertTrue("Mutator is not in the expected state", mutator.deleteIfNull);
    }

    @Test
    public void testConstructorDeleteIfNullFromPolicyState() {
        OperandPolicy policy = new OperandPolicy();
        policy.setDeleteIfNull(true);

        IThriftPool pool = Mockito.mock(IThriftPool.class);
        Mockito.when(pool.getOperandPolicy()).thenReturn(policy);

        Mutator mutator = new Mutator(pool);
        assertTrue("Mutator is not in the expected state", mutator.deleteIfNull);
    }

    @Test
    public void testWriteColumnsWithDeleteIfNullFromConstructor() throws Exception {
        IThriftPool pool = new DebuggingPool(helper.getCluster(), IntegrationTestHelper.KEYSPACE, new OperandPolicy(3, true));

        Bytes rowKey = Bytes.fromLong(Long.MAX_VALUE);

        Bytes colName1 = Bytes.fromInt(1);
        Bytes colName2 = Bytes.fromInt(2);

        Mutator mutator = pool.createMutator();
        assertTrue("Mutator is not in a valid state for this test", mutator.deleteIfNull);
        List<Column> columns = mutator.newColumnList(
                mutator.newColumn(colName1, (Bytes) null),
                mutator.newColumn(colName2, Bytes.fromInt(1))
        );
        mutator.writeColumns(CF, rowKey, columns);

        // make sure there is at least one deletion
        boolean isOneDeletion = false;
        for (Mutation mutation : mutator.getMutationList(CF, rowKey)) {
            if (mutation.isSetDeletion()) {
                isOneDeletion = true;
                break;
            }
        }

        assertTrue("There should be one deletion", isOneDeletion);
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

    @Test
    public void testWriteColumnsDeleteIfNullDisabled() throws Exception {
        Bytes rowKey = Bytes.fromLong(Long.MAX_VALUE);

        Mutator mutator = helper.getPool().createMutator();
        assertFalse("Mutator is not in a valid state for this test", mutator.deleteIfNull);
        List<Column> columns = mutator.newColumnList(
                mutator.newColumn(Bytes.fromInt(1), (Bytes) null)
        );
        mutator.writeColumns(CF, rowKey, columns);

        try {
            mutator.execute(ConsistencyLevel.ONE);
            fail("Should not reach here...");
        } catch (TProtocolException e) {
            assertTrue("Wrong exception thrown", e.getMessage().contains("Required field 'value' was not present!"));
        }
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

    @Test
    public void testWriteSubColumnsDeleteIfNullDisabled() throws Exception {
        Bytes rowKey = Bytes.fromLong(Long.MAX_VALUE);
        Bytes columnName = Bytes.fromShort(Short.MAX_VALUE);

        Mutator mutator = helper.getPool().createMutator();

        assertFalse("Mutator is not in a valid state for this test", mutator.deleteIfNull);
        List<Column> columns = mutator.newColumnList(
                mutator.newColumn(Bytes.fromInt(1), (Bytes) null)
        );

        mutator.writeSubColumns(SCF, rowKey, columnName, columns);

        try {
            mutator.execute(ConsistencyLevel.ONE);
            fail("Should not reach here...");
        } catch (TProtocolException e) {
            assertTrue("Wrong exception thrown", e.getMessage().contains("Required field 'value' was not present!"));
        }
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

    /*private void verifyWriteColumnsMutations(List<Column> columns, Mutator.MutationList mutations, boolean deleteIfNull) {
        assertEquals("Wrong number of mutations", columns.size(), mutations.size());
        for (Column column : columns) {
            Mutation mutation = null;
            for (Mutation candidate : mutations) {
                byte[] candidateColumnName = candidate.isSetColumn_or_supercolumn() ?
                        candidate.getColumn_or_supercolumn().getColumn().getName() :
                        candidate.getDeletion().getPredicate().getColumn_namesIterator().next();
                if (Arrays.equals(column.getName(), candidateColumnName))
                    mutation = candidate;
            }

            assertNotNull("The mutation should not be null", mutation);

            if (!column.isSetValue()) {
                if (deleteIfNull)
                    assertTrue("The mutation should have a deletion if deleteIfNull is true", mutation.isSetDeletion());
                else
                    assertFalse("The mutation should NOT have a deletion the column has a value", mutation.isSetDeletion());
            }
        }
    }*/
}
