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

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.scale7.cassandra.pelops;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scale7.cassandra.pelops.support.AbstractIntegrationTest;
import org.scale7.cassandra.pelops.types.CompositeType;

/**
 *
 * @author Ali Serghini
 */
public class CompositeTypeIntegrationTest extends AbstractIntegrationTest {

    private static final String CF = "CF_CompositeKey";
    private static final List<Bytes> KEYS = new ArrayList<Bytes>(12);

    public CompositeTypeIntegrationTest() {
    }
    
	@BeforeClass
	public static void setup() throws Exception {
        setup(Arrays.asList(new CfDef(KEYSPACE, CF)
                .setComparator_type("CompositeType(LongType,UTF8Type)")
                .setKey_validation_class("LongType")));
        
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

    @Override
    public void prepareData() throws Exception {
        final Mutator mutator = createMutator();
        final List<Column> columns = new ArrayList<Column>(KEYS.size());
        for (Bytes bytes : KEYS) {
            columns.add(mutator.newColumn(bytes));
        }

        mutator.writeColumns(CF, Bytes.fromLong(1l), columns);
        mutator.execute(ConsistencyLevel.ONE);
    }

    @Test
    public void testAdd() {
        // data is setup in the prepareData method...
        int count = createSelector().getColumnCount(CF, Bytes.fromLong(1l), ConsistencyLevel.ONE);
        assertEquals(KEYS.size(), count);
    }

    @Test
    public void testGet() {
        List<Column> columns = createSelector().getColumnsFromRow(CF, Bytes.fromLong(1l), false, ConsistencyLevel.ONE);
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

        List<Bytes> bytes = createSelector().getPageOfColumnNamesFromRow(CF, Bytes.fromLong(1l), builder.build(), false, 20, ConsistencyLevel.ONE);
        assertNotNull(bytes);
        assertEquals(4, bytes.size());

        for (Bytes b : bytes) {
            List<byte[]> bb = CompositeType.parse(b);
            System.out.println("=> first: " + Bytes.fromByteArray(bb.get(0)).toLong() + " second: " + Bytes.fromByteArray(bb.get(1)).toUTF8());
        }
    }
}
