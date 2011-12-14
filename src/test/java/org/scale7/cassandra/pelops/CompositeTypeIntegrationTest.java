/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.scale7.cassandra.pelops;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.*;
import static org.junit.Assert.*;
import org.scale7.cassandra.pelops.types.CompositeType;

/**
 *
 * @author Ali Serghini
 */
public class CompositeTypeIntegrationTest {

    private static final String KS = "SUPER_TEST_KEY_SPACE";
    private static final String CF = "CF_CompositeKey";
    private static final String POOL = "CompositeKeyIntegrationTestPool";
    private static final Cluster CLUSTER = new Cluster("localhost", 9160);
    private static final List<Bytes> KEYS = new ArrayList<Bytes>(12);

    public CompositeTypeIntegrationTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        if (Pelops.getDbConnPool(POOL) != null) {
            Pelops.removePool(POOL);
        }
        Pelops.addPool(POOL, CLUSTER, KS);

        final ColumnFamilyManager columnFamilyManager = Pelops.createColumnFamilyManager(CLUSTER, KS);
        final CfDef columnFamilyDef = new CfDef(KS, CF);
        columnFamilyDef.setComparator_type("CompositeType(LongType,UTF8Type)");
        columnFamilyDef.setKey_validation_class("LongType");
        //columnFamilyDef.setColumn_type(columnType);
        columnFamilyManager.addColumnFamily(columnFamilyDef);


        final CompositeType.Builder builder = CompositeType.Builder.newBuilder(2);
        KEYS.add(builder.addLong(1l).addUTF8("a").build());
        builder.clear();
        KEYS.add(builder.addLong(1l).addUTF8("b").build());
        builder.clear();
        KEYS.add(builder.addLong(1l).addUTF8("c").build());
        builder.clear();
        KEYS.add(builder.addLong(2l).addUTF8("a").build());
        builder.clear();
        KEYS.add(builder.addLong(2l).addUTF8("b").build());
        builder.clear();
        KEYS.add(builder.addLong(2l).addUTF8("c").build());
        builder.clear();
        KEYS.add(builder.addLong(3l).addUTF8("a").build());
        builder.clear();
        KEYS.add(builder.addLong(3l).addUTF8("b").build());
        builder.clear();
        KEYS.add(builder.addLong(3l).addUTF8("c").build());
        builder.clear();
        KEYS.add(builder.addLong(4l).addUTF8("a").build());
        builder.clear();
        KEYS.add(builder.addLong(4l).addUTF8("b").build());
        builder.clear();
        KEYS.add(builder.addLong(4l).addUTF8("c").build());
        builder.clear();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        final ColumnFamilyManager columnFamilyManager = Pelops.createColumnFamilyManager(CLUSTER, KS);
        columnFamilyManager.dropColumnFamily(CF);
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testAdd() {
        final Mutator mutator = Pelops.createMutator(POOL);
        final List<Column> columns = new ArrayList<Column>(KEYS.size());
        for (Bytes bytes : KEYS) {
            columns.add(mutator.newColumn(bytes));
        }

        mutator.writeColumns(CF, Bytes.fromLong(1l), columns);
        mutator.execute(ConsistencyLevel.ONE);

        int count = Pelops.createSelector(POOL).getColumnCount(CF, Bytes.fromLong(1l), ConsistencyLevel.ONE);
        assertEquals(columns.size(), count);
    }

    @Test
    public void testGet() {
        List<Column> columns = Pelops.createSelector(POOL).getColumnsFromRow(CF, Bytes.fromLong(1l), false, ConsistencyLevel.ONE);
        assertNotNull(columns);
        assertEquals(columns.size(), KEYS.size());

        List<byte[]> bytes = null;
        long first = 1l;
        String second = "a";
        for (Column column : columns) {
            bytes = CompositeType.parse(column.getName());
            assertArrayEquals(bytes.get(0), Bytes.fromLong(first).toByteArray());
            assertArrayEquals(bytes.get(1), Bytes.fromUTF8(second).toByteArray());

            if (Arrays.equals(bytes.get(1), Bytes.fromUTF8("c").toByteArray())) {
                first++;
                second = "a";
            } else if (Arrays.equals(bytes.get(1), Bytes.fromUTF8("a").toByteArray())) {
                second = "b";
            } else if (Arrays.equals(bytes.get(1), Bytes.fromUTF8("b").toByteArray())) {
                second = "c";
            } else {
                fail();
            }
        }
    }

    @Test
    public void testSlice() {
        final CompositeType.Builder builder = CompositeType.Builder.newBuilder(2);
        builder.addLong(3l).addUTF8("b");

        List<Bytes> bytes = Pelops.createSelector(POOL).getPageOfColumnNamesFromRow(CF, Bytes.fromLong(1l), builder.build(), false, 20, ConsistencyLevel.ONE);
        assertNotNull(bytes);
        assertEquals(4, bytes.size());

        for (Bytes b : bytes) {
            List<byte[]> bb = CompositeType.parse(b);
            System.out.println("=> first: " + Bytes.fromByteArray(bb.get(0)).toLong() + " second: " + Bytes.fromByteArray(bb.get(1)).toUTF8());
        }
    }
}
