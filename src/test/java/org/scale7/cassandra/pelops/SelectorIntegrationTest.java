package org.scale7.cassandra.pelops;

import org.apache.cassandra.thrift.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static org.scale7.cassandra.pelops.Bytes.fromBytes;
import static org.scale7.cassandra.pelops.Bytes.fromChar;
import static org.scale7.cassandra.pelops.Bytes.fromLong;
import static org.scale7.cassandra.pelops.ColumnFamilyManager.*;

/**
 * Tests the {@link org.scale7.cassandra.pelops.Selector} class.
 */
public class SelectorIntegrationTest {
    private static final Logger logger = SystemProxy.getLoggerFromFactory(SelectorIntegrationTest.class);

    public static final String KEYSPACE = "Pelops-Testing";
    public static final String CF = "SEL_CF";
    public static final String SCF = "SEL_SCF";

    private static Cluster cluster = new Cluster("localhost", 9160);
    private static IThriftPool pool;

    @BeforeClass
    public static void setupPool() throws Exception {
        KeyspaceManager keyspaceManager = new KeyspaceManager(cluster);

        // start from scratch
        if (keyspaceManager.getKeyspaceNames().contains(KEYSPACE)) {
            keyspaceManager.dropKeyspace(KEYSPACE);
        }


        KsDef keyspaceDefinition = new KsDef(KEYSPACE, KeyspaceManager.KSDEF_STRATEGY_RACK_UNAWARE, 1, new ArrayList<CfDef>());

        // add a standard column family
        keyspaceDefinition.addToCf_defs(
                new CfDef(KEYSPACE, CF)
                        .setColumn_type(CFDEF_TYPE_STANDARD)
                        .setComparator_type(CFDEF_COMPARATOR_BYTES)
        );

        // add a super column family
        keyspaceDefinition.addToCf_defs(
                new CfDef(KEYSPACE, SCF)
                        .setColumn_type(CFDEF_TYPE_SUPER)
                        .setComparator_type(CFDEF_COMPARATOR_BYTES)
                        .setSubcomparator_type(CFDEF_COMPARATOR_BYTES)
        );

        keyspaceManager.addKeyspace(keyspaceDefinition);

        pool = new DebuggingPool(cluster, KEYSPACE, new OperandPolicy());

        prepareData();
    }

    private static void prepareData() throws Exception {
        // prep the column family data
        Mutator mutator = pool.createMutator();
        for (long i = 0; i < 100; i++) {
            mutator.writeColumns(CF, fromLong(i), createAlphabetColumns(mutator));
        }

        // prep the super column family data
        for (long i = 0; i < 100; i++) {
            for (char letter = 'A'; letter <= 'Z'; letter++) {
                mutator.writeSubColumns(SCF, fromLong(i), fromChar(letter), createAlphabetColumns(mutator));
            }
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

    @AfterClass
    public static void teardownPool() {
        pool.shutdown();
    }

    @Test
    public void testGetPageOfColumnsFromRow() throws Exception {
        Selector selector = pool.createSelector();
        char[] expectedColumns = new char[] { 'a', 'b', 'c', 'd', 'e' };
        List<Column> columns = selector.getPageOfColumnsFromRow(CF, fromLong(25l), null, false, expectedColumns.length, ConsistencyLevel.ONE);

        veryifyColumns(expectedColumns, columns);
    }

    @Test
    public void testGetPageOfColumnsFromRowWithOffset() throws Exception {
        Selector selector = pool.createSelector();
        char[] expectedColumns = new char[] { 'f', 'g', 'h', 'i', 'j' };
        List<Column> columns = selector.getPageOfColumnsFromRow(CF, fromLong(25l), fromChar('e'), false, expectedColumns.length, ConsistencyLevel.ONE);

        veryifyColumns(expectedColumns, columns);
    }

    @Test
    public void testGetPageOfColumnsFromRowWithNoMatches() throws Exception {
        Selector selector = pool.createSelector();
        char[] expectedColumns = new char[] { };
        List<Column> columns = selector.getPageOfColumnsFromRow(CF, fromLong(25l), fromChar('z'), false, expectedColumns.length, ConsistencyLevel.ONE);

        veryifyColumns(expectedColumns, columns);
    }

    @Test
    public void testGetPageOfColumnsFromRowWithOffsetAndInsufficientMatches() throws Exception {
        Selector selector = pool.createSelector();
        char[] expectedColumns = new char[] { 'x', 'y', 'z' };
        List<Column> columns = selector.getPageOfColumnsFromRow(CF, fromLong(25l), fromChar('w'), false, 1000, ConsistencyLevel.ONE);

        veryifyColumns(expectedColumns, columns);
    }

    @Test
    public void testGetPageOfColumnsFromRowWithOffsetThatDoesNotExist() throws Exception {
        Selector selector = pool.createSelector();
        char[] expectedColumns = new char[] { 'a', 'b', 'c', 'd', 'e' };
        List<Column> columns = selector.getPageOfColumnsFromRow(CF, fromLong(25l), fromChar('`'), false, expectedColumns.length, ConsistencyLevel.ONE);

        veryifyColumns(expectedColumns, columns);
    }

    @Test
    public void testGetPageOfColumnsFromRowWithOffsetThatDoesNotExistAndInsufficientMatches() throws Exception {
        Selector selector = pool.createSelector();
        List<Column> columns = selector.getPageOfColumnsFromRow(CF, fromLong(25l), fromChar('`'), false, 1000, ConsistencyLevel.ONE);

        assertEquals("Wrong number of columns returned", 26, columns.size());
    }

    @Test
    public void testGetPageOfColumnsFromRowReverse() throws Exception {
        Selector selector = pool.createSelector();
        char[] expectedColumns = new char[] { 'z', 'y', 'x', 'w', 'v' };
        List<Column> columns = selector.getPageOfColumnsFromRow(CF, fromLong(25l), null, true, expectedColumns.length, ConsistencyLevel.ONE);

        veryifyColumns(expectedColumns, columns);
    }

    @Test
    public void testGetPageOfColumnsFromRowReverseWithOffset() throws Exception {
        Selector selector = pool.createSelector();
        char[] expectedColumns = new char[] { 'u', 't', 's', 'r', 'q' };
        List<Column> columns = selector.getPageOfColumnsFromRow(CF, fromLong(25l), fromChar('v'), true, expectedColumns.length, ConsistencyLevel.ONE);

        veryifyColumns(expectedColumns, columns);
    }

    private void veryifyColumns(char[] expectedColumns, List<Column> columns) {
        assertEquals("Wrong number of columns returned", expectedColumns.length, columns.size());
        for (int i = 0; i < expectedColumns.length; i++) {
            assertEquals("Wrong column value returned", expectedColumns[i], fromBytes(columns.get(i).getValue()).toChar());
        }
    }

    @Test
    public void testGetPageOfSuperColumnsFromRow() throws Exception {
        Selector selector = pool.createSelector();
        char[] expectedColumns = new char[] { 'A', 'B', 'C', 'D', 'E' };
        List<SuperColumn> superColumns = selector.getPageOfSuperColumnsFromRow(SCF, fromLong(50l), null, false, expectedColumns.length, ConsistencyLevel.ONE);

        verifySuperColumns(expectedColumns, superColumns);
    }

    @Test
    public void testGetPageOfSuperColumnsFromRowWithOffset() throws Exception {
        Selector selector = pool.createSelector();
        char[] expectedColumns = new char[] { 'F', 'G', 'H', 'I', 'J' };
        List<SuperColumn> superColumns = selector.getPageOfSuperColumnsFromRow(SCF, fromLong(50l), fromChar('E'), false, expectedColumns.length, ConsistencyLevel.ONE);

        verifySuperColumns(expectedColumns, superColumns);
    }

    @Test
    public void testGetPageOfSuperColumnsFromRowWithOffsetAndInsufficientMatches() throws Exception {
        Selector selector = pool.createSelector();
        char[] expectedColumns = new char[] { 'X', 'Y', 'Z' };
        List<SuperColumn> superColumns = selector.getPageOfSuperColumnsFromRow(SCF, fromLong(50l), fromChar('W'), false, expectedColumns.length, ConsistencyLevel.ONE);

        verifySuperColumns(expectedColumns, superColumns);
    }

    @Test
    public void testGetPageOfSuperColumnsFromRowWithOffsetThatDoesNotExist() throws Exception {
        Selector selector = pool.createSelector();
        char[] expectedColumns = new char[] { 'A', 'B', 'C', 'D', 'E' };
        List<SuperColumn> columns = selector.getPageOfSuperColumnsFromRow(SCF, fromLong(25l), fromChar('@'), false, expectedColumns.length, ConsistencyLevel.ONE);

        verifySuperColumns(expectedColumns, columns);
    }

    @Test
    public void testGetPageOfSuperColumnsFromRowReverse() throws Exception {
        Selector selector = pool.createSelector();
        char[] expectedColumns = new char[] { 'Z', 'Y', 'X', 'W', 'V' };
        List<SuperColumn> superColumns = selector.getPageOfSuperColumnsFromRow(SCF, fromLong(50l), null, true, expectedColumns.length, ConsistencyLevel.ONE);

        verifySuperColumns(expectedColumns, superColumns);
    }

    @Test
    public void testGetPageOfSuperColumnsFromRowReverseWithOffset() throws Exception {
        Selector selector = pool.createSelector();
        char[] expectedColumns = new char[] { 'U', 'T', 'S', 'R', 'Q' };
        List<SuperColumn> superColumns = selector.getPageOfSuperColumnsFromRow(SCF, fromLong(50l), fromChar('V'), true, expectedColumns.length, ConsistencyLevel.ONE);

        verifySuperColumns(expectedColumns, superColumns);
    }

    private void verifySuperColumns(char[] expectedColumns, List<SuperColumn> superColumns) {
        assertEquals("Wrong number of super columns returned", expectedColumns.length, superColumns.size());
        for (int i = 0; i < expectedColumns.length; i++) {
            assertEquals("Wrong super column value returned", expectedColumns[i], fromBytes(superColumns.get(i).getName()).toChar());
        }
    }
}
