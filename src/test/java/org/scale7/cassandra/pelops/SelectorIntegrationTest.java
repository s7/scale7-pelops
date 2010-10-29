package org.scale7.cassandra.pelops;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;
import static org.scale7.cassandra.pelops.Bytes.fromBytes;
import static org.scale7.cassandra.pelops.Bytes.fromChar;
import static org.scale7.cassandra.pelops.Bytes.fromLong;
import static org.scale7.cassandra.pelops.Bytes.fromUTF8;
import static org.scale7.cassandra.pelops.Bytes.toUTF8;
import static org.scale7.cassandra.pelops.ColumnFamilyManager.CFDEF_COMPARATOR_BYTES;
import static org.scale7.cassandra.pelops.ColumnFamilyManager.CFDEF_COMPARATOR_LONG;
import static org.scale7.cassandra.pelops.ColumnFamilyManager.CFDEF_TYPE_STANDARD;
import static org.scale7.cassandra.pelops.ColumnFamilyManager.CFDEF_TYPE_SUPER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SuperColumn;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scale7.cassandra.pelops.support.AbstractIntegrationTest;

/**
 * Tests the {@link org.scale7.cassandra.pelops.Selector} class.
 */
public class SelectorIntegrationTest extends AbstractIntegrationTest {

    public static final String CF = "SEL_CF";
    public static final String CF_INDEXED = "SEL_I_CF";
    public static final String SCF = "SEL_SCF";
    
	@BeforeClass
	public static void setup() throws Exception {
		setup(Arrays.asList(
                new CfDef(KEYSPACE, CF_INDEXED)
				.setColumn_type(CFDEF_TYPE_STANDARD)
				.setComparator_type(CFDEF_COMPARATOR_BYTES)
				.setDefault_validation_class(CFDEF_COMPARATOR_BYTES)
				.setColumn_metadata(Arrays.asList(
						new ColumnDef(Bytes.fromUTF8("name").getBytes(), CFDEF_COMPARATOR_BYTES)
							// using default CF validation class (CFDEF_COMPARATOR_BYTES)
							.setIndex_name("NameIndex")
							.setIndex_type(IndexType.KEYS),
						new ColumnDef(Bytes.fromUTF8("age").getBytes(), CFDEF_COMPARATOR_LONG)
							.setValidation_class(CFDEF_COMPARATOR_LONG)
							.setIndex_name("AgeIndex")
							.setIndex_type(IndexType.KEYS))),
        new CfDef(KEYSPACE, CF)
                .setColumn_type(CFDEF_TYPE_STANDARD)
                .setComparator_type(CFDEF_COMPARATOR_BYTES),
        new CfDef(KEYSPACE, SCF)
                .setColumn_type(CFDEF_TYPE_SUPER)
                .setComparator_type(CFDEF_COMPARATOR_BYTES)
                .setSubcomparator_type(CFDEF_COMPARATOR_BYTES)
    	));
	}    
    
    @Override
    public void prepareData() throws Exception {
        Mutator mutator = createMutator();
        
        // prep the column family data
        for (long i = 0; i < 100; i++) {
            mutator.writeColumns(CF, fromLong(i), createAlphabetColumns(mutator));

            // prep the super column family data
            for (char letter = 'A'; letter <= 'Z'; letter++) {
                mutator.writeSubColumns(SCF, fromLong(i), fromChar(letter), createAlphabetColumns(mutator));
            }
        }

        // prep indexed column family data
        for (long i = 0; i <= 2; i++) {
            mutator.writeColumn(CF_INDEXED, fromLong(i), mutator.newColumn("name", fromUTF8("name-" + (char)('a' + i))));
            mutator.writeColumn(CF_INDEXED, fromLong(i), mutator.newColumn("age", fromLong(i % 2)));
        }
        
        mutator.execute(ConsistencyLevel.ONE);
    }

    private static List<Column> createAlphabetColumns(Mutator mutator) {
        List<Column> columns = new ArrayList<Column>();
        for (char letter = 'a'; letter <= 'z'; letter++) {
            columns.add(mutator.newColumn(fromChar(letter), fromChar(letter)));
        }

        return columns;
    }

    @Test
    public void testGetIndexedColumns() throws Exception {
    	int MAX_ROWS_IN_RESULT = 3;
    	int MAX_COLUMNS_PER_KEY = 3;

    	Map<Bytes, List<Column>> keys = createSelector().getIndexedColumns(CF_INDEXED,
    			Selector.newIndexClause(Bytes.EMPTY, MAX_ROWS_IN_RESULT,
    					Selector.newIndexExpression("age", IndexOperator.EQ, Bytes.fromLong(1))),
    			Selector.newColumnsPredicateAll(true, MAX_COLUMNS_PER_KEY), ConsistencyLevel.ONE);
    	
    	assertEquals("Wrong number of keys returned", 1, keys.size());
    	
    	Bytes key = keys.keySet().iterator().next();

    	assertEquals("Wrong number key value returned", 1, key.toLong());
    	
    	assertEquals("Wrong number of columns in key returned", 2, keys.get(key).size());
    	
    	Column column = keys.get(key).get(0);
    	
    	assertEquals("Wrong column 'name' value", "name-b", toUTF8(column.getValue()));

    	column = keys.get(key).get(1);
    	
    	assertEquals("Wrong column 'value' value", 1L, Bytes.fromBytes(column.getValue()).toLong());
    }

    @Test
    public void testGetIndexedColumnsMoreResults() throws Exception {
    	int MAX_ROWS_IN_RESULT = 3;
    	int MAX_COLUMNS_PER_KEY = 3;

    	Map<Bytes, List<Column>> keys = createSelector().getIndexedColumns(CF_INDEXED,
    			Selector.newIndexClause(Bytes.EMPTY, MAX_ROWS_IN_RESULT,
    					Selector.newIndexExpression("age", IndexOperator.EQ, Bytes.fromLong(0))),
    			Selector.newColumnsPredicateAll(false, MAX_COLUMNS_PER_KEY), ConsistencyLevel.ONE);
    	
    	assertEquals("Wrong number of keys returned", 2, keys.size());
    	
    	List<Long> keyList = new ArrayList<Long>();
    	
    	for (Bytes key : keys.keySet()) {
    		keyList.add(key.toLong());
    	}
    	
    	Collections.sort(keyList);
    	
    	assertEquals("Wrong key value", 0L, keyList.get(0).longValue());
    	assertEquals("Wrong key value", 2L, keyList.get(1).longValue());
    }
    
    @Test
    public void testGetIndexedColumnsNonEq() throws Exception {
    	int MAX_ROWS_IN_RESULT = 3;
    	int MAX_COLUMNS_PER_KEY = 3;

    	try {
    		Map<Bytes, List<Column>> keys = createSelector().getIndexedColumns(CF_INDEXED,
    			Selector.newIndexClause(Bytes.EMPTY, MAX_ROWS_IN_RESULT,
    					Selector.newIndexExpression("age", IndexOperator.LT, Bytes.fromLong(1))),
    			Selector.newColumnsPredicateAll(true, MAX_COLUMNS_PER_KEY), ConsistencyLevel.ONE);
    	
    		assertEquals("Wrong number of keys returned", 1, keys.size());
    	
    		Bytes key = keys.keySet().iterator().next();

    		assertEquals("Wrong number key value returned", 0, key.toLong());
    	
    		assertEquals("Wrong number of columns in key returned", 2, keys.get(key).size());
    	
    		Column column = keys.get(key).get(0);
    	
    		assertEquals("Wrong column 'name' value", "name-1", toUTF8(column.getValue()));

    		column = keys.get(key).get(1);
    	
    		assertEquals("Wrong column 'value' value", 0L, Bytes.fromBytes(column.getValue()).toLong());
    		
    		fail("Other than EQ index operator is supported now.");
    	}
    	catch (InvalidRequestException e) {
    		assertEquals("No indexed columns present in index clause with operator EQ", e.getWhy());
    	}
    }
    
    @Test
    public void testGetIndexedColumnsNoMatches() throws Exception {
    	int MAX_ROWS_IN_RESULT = 3;
    	int MAX_COLUMNS_PER_KEY = 3;

    	Map<Bytes, List<Column>> keys = createSelector().getIndexedColumns(CF_INDEXED,
    			Selector.newIndexClause(Bytes.EMPTY, MAX_ROWS_IN_RESULT,
    					Selector.newIndexExpression("age", IndexOperator.EQ, Bytes.fromLong(11))),
    			Selector.newColumnsPredicateAll(true, MAX_COLUMNS_PER_KEY), ConsistencyLevel.ONE);
    	
    	assertEquals("Wrong number of keys returned", 0, keys.size());
    }
    
    @Test
    public void testGetPageOfColumnsFromRow() throws Exception {
        char[] expectedColumns = new char[] { 'a', 'b', 'c', 'd', 'e' };
        List<Column> columns = createSelector().getPageOfColumnsFromRow(CF, fromLong(25l), (Bytes) null, false, expectedColumns.length, ConsistencyLevel.ONE);

        verifyColumns(expectedColumns, columns);
    }

    @Test
    public void testGetPageOfColumnsFromRowWithOffset() throws Exception {
        char[] expectedColumns = new char[] { 'f', 'g', 'h', 'i', 'j' };
        List<Column> columns = createSelector().getPageOfColumnsFromRow(CF, fromLong(25l), fromChar('e'), false, expectedColumns.length, ConsistencyLevel.ONE);

        verifyColumns(expectedColumns, columns);
    }

    @Test
    public void testGetPageOfColumnsFromRowWithNoMatches() throws Exception {
        char[] expectedColumns = new char[] { };
        List<Column> columns = createSelector().getPageOfColumnsFromRow(CF, fromLong(25l), fromChar('z'), false, expectedColumns.length, ConsistencyLevel.ONE);

        verifyColumns(expectedColumns, columns);
    }

    @Test
    public void testGetPageOfColumnsFromRowWithOffsetAndInsufficientMatches() throws Exception {
        char[] expectedColumns = new char[] { 'x', 'y', 'z' };
        List<Column> columns = createSelector().getPageOfColumnsFromRow(CF, fromLong(25l), fromChar('w'), false, 1000, ConsistencyLevel.ONE);

        verifyColumns(expectedColumns, columns);
    }

    @Test
    public void testGetPageOfColumnsFromRowWithOffsetThatDoesNotExist() throws Exception {
        char[] expectedColumns = new char[] { 'a', 'b', 'c', 'd', 'e' };
        List<Column> columns = createSelector().getPageOfColumnsFromRow(CF, fromLong(25l), fromChar('`'), false, expectedColumns.length, ConsistencyLevel.ONE);

        verifyColumns(expectedColumns, columns);
    }

    @Test
    public void testGetPageOfColumnsFromRowWithOffsetThatDoesNotExistAndInsufficientMatches() throws Exception {
        List<Column> columns = createSelector().getPageOfColumnsFromRow(CF, fromLong(25l), fromChar('`'), false, 1000, ConsistencyLevel.ONE);

        assertEquals("Wrong number of columns returned", 26, columns.size());
    }

    @Test
    public void testGetPageOfColumnsFromRowReverse() throws Exception {
        char[] expectedColumns = new char[] { 'z', 'y', 'x', 'w', 'v' };
        List<Column> columns = createSelector().getPageOfColumnsFromRow(CF, fromLong(25l), (Bytes) null, true, expectedColumns.length, ConsistencyLevel.ONE);

        verifyColumns(expectedColumns, columns);
    }

    @Test
    public void testGetPageOfColumnsFromRowReverseWithOffset() throws Exception {
        char[] expectedColumns = new char[] { 'u', 't', 's', 'r', 'q' };
        List<Column> columns = createSelector().getPageOfColumnsFromRow(CF, fromLong(25l), fromChar('v'), true, expectedColumns.length, ConsistencyLevel.ONE);

        verifyColumns(expectedColumns, columns);
    }

    private void verifyColumns(char[] expectedColumns, List<Column> columns) {
        assertEquals("Wrong number of columns returned", expectedColumns.length, columns.size());
        for (int i = 0; i < expectedColumns.length; i++) {
            assertEquals("Wrong column value returned", expectedColumns[i], fromBytes(columns.get(i).getValue()).toChar());
        }
    }

    @Test
    public void testGetPageOfSuperColumnsFromRow() throws Exception {
        char[] expectedColumns = new char[] { 'A', 'B', 'C', 'D', 'E' };
        List<SuperColumn> superColumns = createSelector().getPageOfSuperColumnsFromRow(SCF, fromLong(50l), (Bytes) null, false, expectedColumns.length, ConsistencyLevel.ONE);

        verifySuperColumns(expectedColumns, superColumns);
    }

    @Test
    public void testGetPageOfSuperColumnsFromRowWithOffset() throws Exception {
        char[] expectedColumns = new char[] { 'F', 'G', 'H', 'I', 'J' };
        List<SuperColumn> superColumns = createSelector().getPageOfSuperColumnsFromRow(SCF, fromLong(50l), fromChar('E'), false, expectedColumns.length, ConsistencyLevel.ONE);

        verifySuperColumns(expectedColumns, superColumns);
    }

    @Test
    public void testGetPageOfSuperColumnsFromRowWithOffsetAndInsufficientMatches() throws Exception {
        char[] expectedColumns = new char[] { 'X', 'Y', 'Z' };
        List<SuperColumn> superColumns = createSelector().getPageOfSuperColumnsFromRow(SCF, fromLong(50l), fromChar('W'), false, expectedColumns.length, ConsistencyLevel.ONE);

        verifySuperColumns(expectedColumns, superColumns);
    }

    @Test
    public void testGetPageOfSuperColumnsFromRowWithOffsetThatDoesNotExist() throws Exception {
        char[] expectedColumns = new char[] { 'A', 'B', 'C', 'D', 'E' };
        List<SuperColumn> columns = createSelector().getPageOfSuperColumnsFromRow(SCF, fromLong(25l), fromChar('@'), false, expectedColumns.length, ConsistencyLevel.ONE);

        verifySuperColumns(expectedColumns, columns);
    }

    @Test
    public void testGetPageOfSuperColumnsFromRowReverse() throws Exception {
        char[] expectedColumns = new char[] { 'Z', 'Y', 'X', 'W', 'V' };
        List<SuperColumn> superColumns = createSelector().getPageOfSuperColumnsFromRow(SCF, fromLong(50l), (Bytes) null, true, expectedColumns.length, ConsistencyLevel.ONE);

        verifySuperColumns(expectedColumns, superColumns);
    }

    @Test
    public void testGetPageOfSuperColumnsFromRowReverseWithOffset() throws Exception {
        char[] expectedColumns = new char[] { 'U', 'T', 'S', 'R', 'Q' };
        List<SuperColumn> superColumns = createSelector().getPageOfSuperColumnsFromRow(SCF, fromLong(50l), fromChar('V'), true, expectedColumns.length, ConsistencyLevel.ONE);

        verifySuperColumns(expectedColumns, superColumns);
    }

    private void verifySuperColumns(char[] expectedColumns, List<SuperColumn> superColumns) {
        assertEquals("Wrong number of super columns returned", expectedColumns.length, superColumns.size());
        for (int i = 0; i < expectedColumns.length; i++) {
            assertEquals("Wrong super column value returned", expectedColumns[i], fromBytes(superColumns.get(i).getName()).toChar());
        }
    }
}
