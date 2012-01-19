/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.scale7.cassandra.pelops.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scale7.cassandra.pelops.Bytes;

/**
 *
 * @author Ali Serghini
 */
public class CompositeTypeTest {

    public CompositeTypeTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void test() {
        final CompositeType.Builder builder = CompositeType.Builder.newBuilder(13);

        builder.addBoolean(true);
        builder.addByte((byte) 5);
        builder.addByteArray("byte[] test".getBytes());
        builder.addByteBuffer(ByteBuffer.wrap("ByteBuffer test".getBytes()));
        builder.addBytes(Bytes.fromByteArray("Bytes test".getBytes()));
        builder.addChar('c');
        builder.addDouble(123456789d);
        builder.addFloat(5468465f);
        builder.addInt(123);
        builder.addLong(264894651l);
        builder.addShort((short) 1);
        builder.addUTF8("utf8");
        String uuid = UUID.randomUUID().toString();
        builder.addUuid(uuid);
        Bytes bytes = builder.build();

        List<byte[]> parsedBytes = CompositeType.parse(bytes);

        assertNotNull(parsedBytes);
        assertEquals(13, parsedBytes.size());

        assertTrue(Arrays.equals(parsedBytes.get(0), Bytes.fromBoolean(true).toByteArray()));
        assertTrue(Arrays.equals(parsedBytes.get(1), Bytes.fromByte((byte) 5).toByteArray()));
        assertTrue(Arrays.equals(parsedBytes.get(2), "byte[] test".getBytes()));
        assertTrue(Arrays.equals(parsedBytes.get(3), "ByteBuffer test".getBytes()));
        assertTrue(Arrays.equals(parsedBytes.get(4), "Bytes test".getBytes()));
        assertTrue(Arrays.equals(parsedBytes.get(5), Bytes.fromChar('c').toByteArray()));
        assertTrue(Arrays.equals(parsedBytes.get(6), Bytes.fromDouble(123456789d).toByteArray()));
        assertTrue(Arrays.equals(parsedBytes.get(7), Bytes.fromFloat(5468465f).toByteArray()));
        assertTrue(Arrays.equals(parsedBytes.get(8), Bytes.fromInt(123).toByteArray()));
        assertTrue(Arrays.equals(parsedBytes.get(9), Bytes.fromLong(264894651l).toByteArray()));
        assertTrue(Arrays.equals(parsedBytes.get(10), Bytes.fromShort((short) 1).toByteArray()));
        assertTrue(Arrays.equals(parsedBytes.get(11), Bytes.fromUTF8("utf8").toByteArray()));
        assertTrue(Arrays.equals(parsedBytes.get(12), Bytes.fromUuid(uuid).toByteArray()));
    }
}
