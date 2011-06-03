package org.scale7.cassandra.pelops;

import com.google.common.collect.Lists;
import org.apache.cassandra.thrift.*;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scale7.cassandra.pelops.exceptions.InvalidRequestException;
import org.scale7.cassandra.pelops.support.AbstractIntegrationTest;

import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.scale7.cassandra.pelops.Bytes.*;
import static org.scale7.cassandra.pelops.ColumnFamilyManager.*;

/**
 * Tests the {@link org.scale7.cassandra.pelops.Selector} class.
 */
public class SelectorIntegrationTest extends AbstractIntegrationTest {

    public static final String CF = "SEL_CF";
    public static final String CF_KEY_ITERATOR = "SEL_CF_KI";
    public static final String CF_INDEXED = "SEL_I_CF";
    public static final String CF_COUNTER = "SEL_C_CF";
    public static final String SCF = "SEL_SCF";
    
	@BeforeClass
	public static void setup() throws Exception {
		setup(
                Arrays.asList(
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
                        new CfDef(KEYSPACE, CF_KEY_ITERATOR)
                                .setColumn_type(CFDEF_TYPE_STANDARD)
                                .setComparator_type(CFDEF_COMPARATOR_BYTES),
                        new CfDef(KEYSPACE, CF_COUNTER)
                                .setColumn_type(CFDEF_TYPE_STANDARD)
                                .setComparator_type(CFDEF_COMPARATOR_BYTES)
                                .setDefault_validation_class(CFDEF_VALIDATION_CLASS_COUNTER),
                        new CfDef(KEYSPACE, SCF)
                                .setColumn_type(CFDEF_TYPE_SUPER)
                                .setComparator_type(CFDEF_COMPARATOR_BYTES)
                                .setSubcomparator_type(CFDEF_COMPARATOR_BYTES)
                )
        );
	}    
    
    @Override
    public void prepareData() throws Exception {
        Mutator mutator = createMutator();
        
        // prep the column family data
        for (long i = 0; i < 100; i++) {
            mutator.writeColumns(CF, fromLong(i), createAlphabetColumns(mutator));
            mutator.writeColumns(CF_KEY_ITERATOR, fromLong(i), createAlphabetColumns(mutator));
            mutator.writeCounterColumn(CF_COUNTER, fromLong(i), fromLong(i), i);
            mutator.writeCounterColumn(CF_COUNTER, fromLong(i), fromLong(i + 1), i);

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

    private static char[] createAlphabet() {
        char[] letters = new char[26];
        int index = 0;
        for (char letter = 'a'; letter <= 'z'; letter++) {
            letters[index++] = letter;
        }

        return letters;
    }

    @Test
    public void testGetCounterColumnFromRow() {
        CounterColumn counterColumn = createSelector().getCounterColumnFromRow(CF_COUNTER, fromLong(50), fromLong(50), ConsistencyLevel.QUORUM);
        assertEquals("Wrong counter column value returned", 50, counterColumn.getValue());
    }

    @Test
    public void testGetCounterColumnsFromRow() {
        List<CounterColumn> columns = createSelector().getCounterColumnsFromRow(CF_COUNTER, fromLong(50), false, ConsistencyLevel.QUORUM);
        assertEquals("Wrong number of columns returned", 2, columns.size());
        assertEquals("Wrong counter column value returned", 50, Selector.getCountColumnValue(columns, fromLong(50), null));
        assertEquals("Wrong counter column value returned", 50, Selector.getCountColumnValue(columns, fromLong(51), null));
    }

    @Test
    public void testGetCounterColumnsFromRows() {
        LinkedHashMap<Bytes, List<CounterColumn>> columnsFromRows = createSelector().getCounterColumnsFromRows(CF_COUNTER, Arrays.asList(fromLong(50), fromLong(51)), false, ConsistencyLevel.QUORUM);
        assertEquals("Wrong number of rows returned", 2, columnsFromRows.size());

        assertEquals("Wrong number of columns returned", 2, columnsFromRows.get(fromLong(50)).size());
        assertEquals("Wrong number of columns returned", 2, columnsFromRows.get(fromLong(51)).size());
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

        assertEquals("Wrong column 'value' value", 1L, Bytes.fromByteArray(column.getValue()).toLong());
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

            assertEquals("Wrong column 'value' value", 0L, Bytes.fromByteArray(column.getValue()).toLong());
    		
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
            assertEquals("Wrong column value returned", expectedColumns[i], Bytes.fromByteArray(columns.get(i).getValue()).toChar());
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

    @Test
    public void testIterateSuperColumnsFromRow() {
        for (char letter = 'A'; letter <= 'Z'; letter++) {
        }
        Iterator<SuperColumn> iterator = createSelector().iterateSuperColumnsFromRow(SCF, fromLong(50l), null, false, 10, ConsistencyLevel.ONE);

        char letter = 'A';
        int count = 0;
        while (iterator.hasNext()) {
            SuperColumn superColumn = iterator.next();

            assertEquals("Wrong super column value returned", letter, Bytes.fromByteArray(superColumn.getName()).toChar());

            letter++;
            count++;
        }

        assertEquals("Not all super columns were processed", 26, count);
    }

    @Test
    public void testIterateSuperColumnsFromRowUsingOnlyNext() {
        Iterator<SuperColumn> iterator = createSelector().iterateSuperColumnsFromRow(SCF, fromLong(50l), null, false, 10, ConsistencyLevel.ONE);

        char letter = 'A';
        int count = 0;
        for (int i = 0; i < 26; i++) {
            SuperColumn superColumn = iterator.next();

            assertEquals("Wrong super column value returned", letter, Bytes.fromByteArray(superColumn.getName()).toChar());

            letter++;
            count++;
        }

        assertEquals("Not all super columns were processed", 26, count);

        try {
            iterator.next();
            fail("The iterator should have thrown a NoSuchElementException exception");
        } catch (NoSuchElementException e) {
            // expected
        }
    }

    @Test
    public void testIterateColumnsFromRow() {
        Iterator<Column> iterator = createSelector().iterateColumnsFromRow(CF, fromLong(50l), null, false, 10, ConsistencyLevel.ONE);

        char letter = 'a';
        int count = 0;
        while (iterator.hasNext()) {
            Column column = iterator.next();

            assertEquals("Wrong column value returned", letter, Bytes.fromByteArray(column.getName()).toChar());

            letter++;
            count++;
        }

        assertEquals("Not all columns were processed", 26, count);
    }

    @Test
    public void testIterateColumnsFromRowUsingOnlyNext() {
        Iterator<Column> iterator = createSelector().iterateColumnsFromRow(CF, fromLong(50l), null, false, 10, ConsistencyLevel.ONE);

        char letter = 'a';
        int count = 0;
        for (int i = 0; i < 26; i++) {
            Column column = iterator.next();

            assertEquals("Wrong column value returned", letter, Bytes.fromByteArray(column.getName()).toChar());

            letter++;
            count++;
        }

        assertEquals("Not all columns were processed", 26, count);

        try {
            iterator.next();
            fail("The iterator should have thrown a NoSuchElementException exception");
        } catch (NoSuchElementException e) {
            // expected
        }
    }

    @Test
    public void testIterateColumnsFromRows() {
        Iterator<Map.Entry<Bytes, List<Column>>> iterator = createSelector().iterateColumnsFromRows(CF_KEY_ITERATOR, 10, ConsistencyLevel.ONE);

        int count = 0;
        Set<Bytes> keys = new HashSet<Bytes>(100);
        while (iterator.hasNext()) {
            Map.Entry<Bytes, List<Column>> row = iterator.next();
            keys.add(row.getKey());
            count++;
        }

        assertEquals("Not all rows were processed", 100, count);

        for (long i = 0; i < 100; i++) {
            final Bytes key = Bytes.fromLong(i);
            assertTrue(String.format("Key %s was missing from the results", i), keys.contains(key));
        }
    }

    @Test
    public void testIterateColumnsFromRowsOnlyUsingNext() {
        Iterator<Map.Entry<Bytes, List<Column>>> iterator = createSelector().iterateColumnsFromRows(CF_KEY_ITERATOR, 10, ConsistencyLevel.ONE);

        Set<Bytes> keys = new HashSet<Bytes>(100);

        for (int i = 0; i < 100; i++) {
            Map.Entry<Bytes, List<Column>> row = iterator.next();
            keys.add(row.getKey());
        }

        for (long i = 0; i < 100; i++) {
            final Bytes key = Bytes.fromLong(i);
            assertTrue(String.format("Key %s was missing from the results", i), keys.contains(key));
        }
    }

    @Test
    public void testGetColumnsFromRowsOrdering() {
        List<Bytes> keys = Lists.newArrayList(fromLong(5), fromLong(6), fromLong(8), fromLong(9), fromLong(7)); // out of order on purpose

        Map<Bytes, List<Column>> columnsFromRows = createSelector().getColumnsFromRows(CF, keys, false, ConsistencyLevel.ONE);

        // make sure the results are in order of the provided keys
        int index = 0;
        for (Bytes bytes : columnsFromRows.keySet()) {
            assertEquals("Results were not in the order of the provided keys", keys.get(index), bytes);

            verifyColumns(createAlphabet(), columnsFromRows.get(bytes));

            index++;
        }
    }

    @Test
    public void testGetColumnsFromRowsMissingKey() {
        final Bytes missingKey = fromLong(Long.MAX_VALUE - 10);
        List<Bytes> keys = Lists.newArrayList(fromLong(5), fromLong(6), fromLong(8), fromLong(9), fromLong(7), missingKey); // out of order on purpose

        Map<Bytes, List<Column>> columnsFromRows = createSelector().getColumnsFromRows(CF, keys, false, ConsistencyLevel.ONE);

        assertTrue("The missing key didn't exist in the results", columnsFromRows.containsKey(missingKey));
        assertNotNull("The missing keys value should be an empty list", columnsFromRows.get(missingKey));
        assertTrue("The missing keys value should be an empty list", columnsFromRows.get(missingKey).isEmpty());
    }

    @Test
    public void testGetSubColumnsFromRowsOrdering() {
        List<Bytes> keys = Lists.newArrayList(fromLong(5), fromLong(6), fromLong(8), fromLong(9), fromLong(7)); // out of order on purpose

        Map<Bytes, List<Column>> columnsFromRows = createSelector().getSubColumnsFromRows(SCF, keys, fromChar('B'), false, ConsistencyLevel.ONE);

        // make sure the results are in order of the provided keys
        int index = 0;
        for (Bytes bytes : columnsFromRows.keySet()) {
            assertEquals("Results were not in the order of the provided keys", keys.get(index), bytes);

            verifyColumns(createAlphabet(), columnsFromRows.get(bytes));

            index++;
        }
    }

    @Test
    public void testGetSubColumnsFromRowsMissingKey() {
        final Bytes missingKey = fromLong(Long.MAX_VALUE - 10);
        List<Bytes> keys = Lists.newArrayList(fromLong(5), fromLong(6), fromLong(8), fromLong(9), fromLong(7), missingKey); // out of order on purpose

        Map<Bytes, List<Column>> columnsFromRows = createSelector().getSubColumnsFromRows(SCF, keys, fromChar('B'), false, ConsistencyLevel.ONE);

        assertTrue("The missing key didn't exist in the results", columnsFromRows.containsKey(missingKey));
        assertNotNull("The missing keys value should be an empty list", columnsFromRows.get(missingKey));
        assertTrue("The missing keys value should be an empty list", columnsFromRows.get(missingKey).isEmpty());
    }


    private void verifySuperColumns(char[] expectedColumns, List<SuperColumn> superColumns) {
        assertEquals("Wrong number of super columns returned", expectedColumns.length, superColumns.size());
        for (int i = 0; i < expectedColumns.length; i++) {
            assertEquals("Wrong super column value returned", expectedColumns[i], Bytes.fromByteArray(superColumns.get(i).getName()).toChar());
        }
    }
}
