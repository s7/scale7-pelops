package org.scale7.cassandra.pelops;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.scale7.cassandra.pelops.ColumnFamilyManager.CFDEF_COMPARATOR_BYTES;
import static org.scale7.cassandra.pelops.ColumnFamilyManager.CFDEF_TYPE_STANDARD;
import static org.scale7.cassandra.pelops.ColumnFamilyManager.CFDEF_TYPE_SUPER;

import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Mutation;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.scale7.cassandra.pelops.exceptions.ModelException;
import org.scale7.cassandra.pelops.pool.DebuggingPool;
import org.scale7.cassandra.pelops.pool.IThriftPool;
import org.scale7.cassandra.pelops.support.AbstractIntegrationTest;

/**
 * Tests the {@link Selector} class.
 */
public class MutatorIntegrationTest extends AbstractIntegrationTest {

	public static final String CF = "MUT_CF";
    public static final String SCF = "MUT_SCF";

	@BeforeClass
	public static void setup() throws Exception {
		setup(Arrays.asList(
                new CfDef(KEYSPACE, CF)
                .setColumn_type(CFDEF_TYPE_STANDARD)
                .setComparator_type(CFDEF_COMPARATOR_BYTES),
        new CfDef(KEYSPACE, SCF)
                .setColumn_type(CFDEF_TYPE_SUPER)
                .setComparator_type(CFDEF_COMPARATOR_BYTES)
                .setSubcomparator_type(CFDEF_COMPARATOR_BYTES)));
	}

    @Test
    public void testConstructorDeleteIfNullState() {
        IThriftPool pool = Mockito.mock(IThriftPool.class);
        Mutator mutator = new Mutator(pool, System.currentTimeMillis(), true);
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
    public void testConstructorArgs() {
        IThriftPool pool = Mockito.mock(IThriftPool.class);

        Mutator mutator = new Mutator(pool, Long.MAX_VALUE, true, Integer.MAX_VALUE);
        assertEquals("Mutator timestamp is not in the expected state", Long.MAX_VALUE, mutator.timestamp);
        assertTrue("Mutator deleteIfNull is not in the expected state", mutator.deleteIfNull);
        assertEquals("Mutator TTL is not in the expected state", Integer.MAX_VALUE, (int) mutator.ttl);
    }

    @Test
    public void testNewColumnWithTTL() {
        IThriftPool pool = Mockito.mock(IThriftPool.class);

        Mutator mutator = new Mutator(pool, Long.MAX_VALUE, true, Integer.MAX_VALUE);
        Column column = mutator.newColumn(Bytes.fromUTF8("a"), Bytes.fromUTF8("b"), 1234);

        assertEquals("column name is not in the expected state", Bytes.fromUTF8("a").getBytes(), column.name);
        assertEquals("column value is not in the expected state", Bytes.fromUTF8("b").getBytes(), column.value);
        assertEquals("column TTL is not in the expected state", 1234, column.ttl);
    }

    @Test
    public void testNewColumnWithTTLDefaultFromMemberVariable() {
        IThriftPool pool = Mockito.mock(IThriftPool.class);

        Mutator mutator = new Mutator(pool, Long.MAX_VALUE, true, Integer.MAX_VALUE);
        Column column = mutator.newColumn(Bytes.fromUTF8("a"), Bytes.fromUTF8("b"));

        assertEquals("column name is not in the expected state", Bytes.fromUTF8("a").getBytes(), column.name);
        assertEquals("column value is not in the expected state", Bytes.fromUTF8("b").getBytes(), column.value);
        assertEquals("column TTL is not in the expected state", Integer.MAX_VALUE, column.ttl);
    }

    @Test
    public void testNewColumnWithTTLNotSet() {
        IThriftPool pool = Mockito.mock(IThriftPool.class);

        Mutator mutator = new Mutator(pool, Long.MAX_VALUE, true, Integer.MAX_VALUE);
        Column column = mutator.newColumn(Bytes.fromUTF8("a"), Bytes.fromUTF8("b"), Mutator.NO_TTL);

        assertEquals("column name is not in the expected state", Bytes.fromUTF8("a").getBytes(), column.name);
        assertEquals("column value is not in the expected state", Bytes.fromUTF8("b").getBytes(), column.value);
        assertFalse("column TTL is not in the expected state", column.isSetTtl());
    }

    @Test
    public void testWriteColumnsWithDeleteIfNullFromConstructor() throws Exception {
        IThriftPool pool = new DebuggingPool(getCluster(), KEYSPACE, new OperandPolicy(3, true));

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

        pool.shutdown();
    }

    /**
     * Tests that a write replaces previous values and that the appropriate columns are deleted when null values are set
     * on the columns.
     */
    @Test
    public void testWriteColumnsWithDeletesReplacingPrevious() throws Exception {
        Bytes rowKey = Bytes.fromLong(Long.MAX_VALUE);

        // write out the value to be replaced (deleted)
        Mutator mutator = getPool().createMutator();
        List<Column> columns = mutator.newColumnList(
                mutator.newColumn(Bytes.fromInt(1), Bytes.fromChar('a')),
                mutator.newColumn(Bytes.fromInt(2), Bytes.fromChar('b')),
                mutator.newColumn(Bytes.fromInt(3), Bytes.fromChar('c'))
        );
        mutator.writeColumns(CF, rowKey, columns);
        mutator.execute(ConsistencyLevel.ONE);

        // make sure the data was written as expected
        Selector selector = createSelector();
        List<Column> persistedColumns = selector.getColumnsFromRow(CF, rowKey, false, ConsistencyLevel.ONE);

        verifyColumns(columns, persistedColumns);

        // write out the replacement values
        mutator = createMutator();
        columns = mutator.newColumnList(
                mutator.newColumn(Bytes.fromInt(1), Bytes.fromChar('d')),
                mutator.newColumn(Bytes.fromInt(2), (Bytes) null),
                mutator.newColumn(Bytes.fromInt(3), Bytes.NULL)
        );
        mutator.writeColumns(CF, rowKey, columns, true);
        mutator.execute(ConsistencyLevel.ONE);

        // make sure the data was written as expected and that the appropriate columns have been deleted
        selector = createSelector();
        persistedColumns = selector.getColumnsFromRow(CF, rowKey, false, ConsistencyLevel.ONE);

        verifyColumns(columns.subList(0, 1), persistedColumns);
    }

    @Test
    public void testWriteColumnsDeleteIfNullDisabled() throws Exception {
        Bytes rowKey = Bytes.fromLong(Long.MAX_VALUE);

        Mutator mutator = createMutator();
        assertFalse("Mutator is not in a valid state for this test", mutator.deleteIfNull);
        List<Column> columns = mutator.newColumnList(
                mutator.newColumn(Bytes.fromInt(1), (Bytes) null)
        );
        try {
            mutator.writeColumns(CF, rowKey, columns);
            fail("Should not reach here...");
        } catch (ModelException e) {
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
        Mutator mutator = createMutator();
        List<Column> columns = mutator.newColumnList(
                mutator.newColumn(Bytes.fromInt(1), Bytes.fromChar('a')),
                mutator.newColumn(Bytes.fromInt(2), Bytes.fromChar('b')),
                mutator.newColumn(Bytes.fromInt(3), Bytes.fromChar('c'))
        );
        mutator.writeSubColumns(SCF, rowKey, columnName, columns);
        mutator.execute(ConsistencyLevel.ONE);

        // make sure the data was written as expected
        Selector selector = createSelector();
        List<Column> persistedColumns = selector.getSubColumnsFromRow(SCF, rowKey, columnName, false, ConsistencyLevel.ONE);

        verifyColumns(columns, persistedColumns);

        // write out the replacement values
        mutator = createMutator();
        columns = mutator.newColumnList(
                mutator.newColumn(Bytes.fromInt(1), Bytes.fromChar('d')),
                mutator.newColumn(Bytes.fromInt(2), (Bytes) null),
                mutator.newColumn(Bytes.fromInt(3), Bytes.NULL)
        );
        mutator.writeSubColumns(SCF, rowKey, columnName, columns, true);
        mutator.execute(ConsistencyLevel.ONE);

        // make sure the data was written as expected and that the appropriate columns have been deleted
        selector = createSelector();
        persistedColumns = selector.getSubColumnsFromRow(SCF, rowKey, columnName, false, ConsistencyLevel.ONE);

        verifyColumns(columns.subList(0, 1), persistedColumns);
    }

    @Test
    public void testWriteSubColumnsDeleteIfNullDisabled() throws Exception {
        Bytes rowKey = Bytes.fromLong(Long.MAX_VALUE);
        Bytes columnName = Bytes.fromShort(Short.MAX_VALUE);

        Mutator mutator = createMutator();

        assertFalse("Mutator is not in a valid state for this test", mutator.deleteIfNull);
        List<Column> columns = mutator.newColumnList(
                mutator.newColumn(Bytes.fromInt(1), (Bytes) null)
        );

        try {
            mutator.writeSubColumns(SCF, rowKey, columnName, columns);
            fail("Should not reach here...");
        } catch (ModelException e) {
        }
    }

    @Test
    public void testDeleteSubColumns() throws Exception {
        Bytes rowKey = Bytes.fromLong(Long.MAX_VALUE);
        Bytes columnName = Bytes.fromShort(Short.MAX_VALUE);

        // write out the value to be deleted
        Mutator mutator = createMutator();
        List<Column> columns = mutator.newColumnList(
                mutator.newColumn(Bytes.fromInt(1), Bytes.fromChar('a')),
                mutator.newColumn(Bytes.fromInt(2), Bytes.fromChar('b')),
                mutator.newColumn(Bytes.fromInt(3), Bytes.fromChar('c'))
        );
        mutator.writeSubColumns(SCF, rowKey, columnName, columns);
        mutator.execute(ConsistencyLevel.ONE);

        // delete all the sub columns
        mutator = createMutator();
        mutator.deleteSubColumns(SCF, rowKey, columnName);
        mutator.execute(ConsistencyLevel.ONE);

        // verify sub columns deleted
        List<Column> subColumnsFromRow = createSelector().getSubColumnsFromRow(SCF, rowKey, columnName, false, ConsistencyLevel.ONE);
        assertThat("Sub columns were not deleted", subColumnsFromRow.size(), is(equalTo(0)));
    }

    @Test
    public void testDeleteSubColumnsWithNullColumnNameThrowsCorrectException() throws Exception {
        Bytes rowKey = Bytes.fromLong(Long.MAX_VALUE);

        // delete all the sub columns
        Mutator mutator = createMutator();
        try {
            mutator.deleteSubColumns(SCF, rowKey, null);
            fail("A ModelException exception should have been thrown");
        } catch (ModelException e) {
        }
    }

    @Test
    public void testTProtocolExceptionDoesNotBreakPooledConnection() throws Exception {
        Bytes rowKey = Bytes.fromLong(Long.MAX_VALUE);

        Mutator mutator = createMutator();

        assertFalse("Mutator is not in a valid state for this test", mutator.deleteIfNull);

        try {
            mutator.writeColumn(CF, rowKey, mutator.newColumn(Bytes.fromInt(1), (Bytes) null));
            mutator.execute(ConsistencyLevel.ONE);
        } catch (Exception e) {
            // do nothing
        }

        createSelector().getColumnsFromRow(CF, rowKey, false, ConsistencyLevel.ONE);
    }

    @Test
    public void testNotFoundExceptionDoesNotBreakPooledConnection() throws Exception {
        Bytes rowKey = Bytes.fromLong(Long.MAX_VALUE);

        Mutator mutator = createMutator();

        assertFalse("Mutator is not in a valid state for this test", mutator.deleteIfNull);

        try {
            createSelector().getColumnsFromRow(CF, rowKey, false, ConsistencyLevel.ONE);
        } catch (Exception e) {
            // do nothing
        }

        createSelector().getColumnsFromRow(CF, rowKey, false, ConsistencyLevel.ONE);
    }

    private void verifyColumns(List<Column> expectedColumns, List<Column> actualColumns) {
        assertEquals("Wrong number of columns", expectedColumns.size(), actualColumns.size());
        for (int i = 0; i < expectedColumns.size(); i++) {
            Column expectedColumn = expectedColumns.get(i);
            Column actualColumn = actualColumns.get(i);

            assertEquals("Column names didn't match", Bytes.fromByteArray(expectedColumn.getName()), Bytes.fromByteArray(actualColumn.getName()));
            assertEquals("Column values didn't match", Bytes.fromByteArray(expectedColumn.getValue()), Bytes.fromByteArray(actualColumn.getValue()));
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
