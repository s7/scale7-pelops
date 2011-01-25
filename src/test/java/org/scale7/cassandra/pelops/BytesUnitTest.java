package org.scale7.cassandra.pelops;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

import static junit.framework.Assert.*;

/**
 * Tests the {@link Bytes} class.
 */
public class BytesUnitTest {
    @Test
    public void testNullArray() {
        Bytes bytes = Bytes.NULL;
        assertNull("The underlying array should not be null", bytes.getBytes());
    }
    
    @Test
    public void testEquals() {
        Bytes one = Bytes.fromByteArray(new byte[]{1, 2, 3, 4, 5});
        Bytes two = Bytes.fromByteArray(new byte[]{1, 2, 3, 4, 5});

        assertTrue("Two instance were not equal", one.equals(two));
    }

    @Test
    public void testEqualsNegative() {
        Bytes one = Bytes.fromByteArray(new byte[]{5, 4, 3, 2, 1});
        Bytes two = Bytes.fromByteArray(new byte[]{1, 2, 3, 4, 5});

        assertFalse("Two instance were not equal", one.equals(two));
    }

    @Test
    public void testHashCode() {
        Bytes one = Bytes.fromByteArray(new byte[]{1, 2, 3, 4, 5});
        Bytes two = Bytes.fromByteArray(new byte[]{1, 2, 3, 4, 5});

        assertEquals("The hashCode method should generate the same value from the same input", one.hashCode(), two.hashCode());
    }

    @Test
    public void testHashCodeDifferent() {
        Bytes one = Bytes.fromByteArray(new byte[]{5, 4, 3, 2, 1});
        Bytes two = Bytes.fromByteArray(new byte[]{1, 2, 3, 4, 5});

        assertFalse("The hashCode method should generate the a different value from different input", one.hashCode() == two.hashCode());
    }

    @Test
    public void testBytes() {
        byte[] value = {1, 2, 3, 4, 5};
        Bytes from = Bytes.fromByteArray(value);
        byte[] to = from.getBytes().array();

        assertTrue("Conversion did not match", Arrays.equals(value, to));
    }

    @Test
    public void testByteArray() {
        byte[] value = {1, 2, 3, 4, 5};
        Bytes from = Bytes.fromByteArray(value);
        byte[] to = from.toByteArray();

        assertTrue("Conversion did not match", Arrays.equals(value, to));
    }

    @Test
    public void testByte() {
        byte value = (byte) 9;
        Bytes from = Bytes.fromByte(value);
        byte to = from.toByte();

        assertEquals("Conversion did not match", value, to);

        // make sure the buffer is rewound
        from.toByte();
    }

    /**
     * The thrift API passes an array full of all kinds of stuff.  This test ensures that the Bytes class operates correctly
     * when this is the case (e.g. when read results from cassandra).
     */
    @Test
    public void testByteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(Bytes.SIZEOF_DOUBLE + Bytes.SIZEOF_BYTE + Bytes.SIZEOF_LONG);
        buffer.putDouble(Double.MAX_VALUE).put((byte) 9).putLong(Long.MAX_VALUE);

        Bytes from = Bytes.fromByteBuffer(
                ByteBuffer.wrap(buffer.array(), Bytes.SIZEOF_DOUBLE + 1, Bytes.SIZEOF_BYTE)
        );
        byte to = from.toByte();

        assertEquals("Conversion did not match", Byte.MAX_VALUE, to);

        // make sure the buffer is rewound
        to = from.toByte();
        
        assertEquals("Conversion did not match", Byte.MAX_VALUE, to);
    }

    @Test
    public void testByteObject() {
        Byte value = (byte) 9;
        Bytes from = Bytes.fromByte(value);
        Byte to = from.toByte(null);

        assertEquals("Conversion did not match", value, to);
    }

    @Test
    public void testByteObjectNull() {
        Byte value = null;
        Bytes from = Bytes.fromByte(value);
        Byte to = from.toByte(null);

        assertEquals("Conversion did not match", value, to);
    }

    @Test
    public void testBooleanTrue() {
        boolean value = true;

        Bytes from = Bytes.fromBoolean(value);
        boolean to = from.toBoolean();

        assertEquals("Conversion did not match", value, to);

        // make sure the buffer is rewound
        from.toBoolean();
    }

    @Test
    public void testBooleanFalse() {
        boolean value = false;

        Bytes from = Bytes.fromBoolean(value);
        boolean to = from.toBoolean();

        assertEquals("Conversion did not match", value, to);

        // make sure the buffer is rewound
        from.toBoolean();
    }

    @Test
    public void testBooleanObject() {
        Boolean value = false;

        Bytes from = Bytes.fromBoolean(value);
        Boolean to = from.toBoolean(null);

        assertEquals("Conversion did not match", value, to);
    }

    @Test
    public void testBooleanObjectNull() {
        Boolean value = null;

        Bytes from = Bytes.fromBoolean(value);
        Boolean to = from.toBoolean(null);

        assertEquals("Conversion did not match", value, to);
    }

    @Test
    public void testChar() {
        char value = 'a';
        Bytes from = Bytes.fromChar(value);
        char to = from.toChar();

        assertEquals("Conversion did not match", value, to);

        // make sure the buffer is rewound
        from.toChar();
    }

    @Test
    public void testCharObject() {
        Character value = 'a';
        Bytes from = Bytes.fromChar(value);
        Character to = from.toChar(null);

        assertEquals("Conversion did not match", value, to);
    }

    @Test
    public void testCharObjectNull() {
        Character value = null;
        Bytes from = Bytes.fromChar(value);
        Character to = from.toChar(null);

        assertEquals("Conversion did not match", value, to);
    }

    @Test
    public void testShort() {
        short value = Short.MAX_VALUE;
        Bytes from = Bytes.fromShort(value);
        short to = from.toShort();

        assertEquals("Conversion did not match", value, to);

        // make sure the buffer is rewound
        from.toShort();
    }

    @Test
    public void testShortObject() {
        Short value = Short.MAX_VALUE;
        Bytes from = Bytes.fromShort(value);
        Short to = from.toShort(null);

        assertEquals("Conversion did not match", value, to);
    }

    @Test
    public void testShortObjectNull() {
        Short value = null;
        Bytes from = Bytes.fromShort(value);
        Short to = from.toShort(null);

        assertEquals("Conversion did not match", value, to);
    }

    @Test
    public void testInt() {
        int value = Integer.MAX_VALUE;
        Bytes from = Bytes.fromInt(value);
        int to = from.toInt();

        assertEquals("Conversion did not match", value, to);

        // make sure the buffer is rewound
        from.toInt();
    }

    @Test
    public void testIntObject() {
        Integer value = Integer.MAX_VALUE;
        Bytes from = Bytes.fromInt(value);
        Integer to = from.toInt(null);

        assertEquals("Conversion did not match", value, to);
    }

    @Test
    public void testIntObjectNull() {
        Integer value = null;
        Bytes from = Bytes.fromInt(value);
        Integer to = from.toInt(null);

        assertEquals("Conversion did not match", value, to);
    }

    @Test
    public void testLong() {
        long value = Long.MAX_VALUE;
        Bytes from = Bytes.fromLong(value);
        long to = from.toLong();

        assertEquals("Conversion did not match", value, to);

        // make sure the buffer is rewound
        from.toLong();
    }

    @Test
    public void testLongObject() {
        Long value = Long.MAX_VALUE;
        Bytes from = Bytes.fromLong(value);
        Long to = from.toLong(null);

        assertEquals("Conversion did not match", value, to);
    }

    @Test
    public void testLongObjectNull() {
        Long value = null;
        Bytes from = Bytes.fromLong(value);
        Long to = from.toLong(null);

        assertEquals("Conversion did not match", value, to);
    }

    @Test
    public void testFloat() {
        float value = Float.MAX_VALUE;
        Bytes from = Bytes.fromFloat(value);
        float to = from.toFloat();

        assertEquals("Conversion did not match", value, to);

        // make sure the buffer is rewound
        from.toFloat();
    }

    @Test
    public void testFloatObject() {
        Float value = Float.MAX_VALUE;
        Bytes from = Bytes.fromFloat(value);
        Float to = from.toFloat(null);

        assertEquals("Conversion did not match", value, to);
    }

    @Test
    public void testFloatObjectNull() {
        Float value = Float.MAX_VALUE;
        Bytes from = Bytes.fromFloat(value);
        Float to = from.toFloat(null);

        assertEquals("Conversion did not match", value, to);
    }

    @Test
    public void testDouble() {
        double value = Double.MAX_VALUE;
        Bytes from = Bytes.fromDouble(value);
        double to = from.toDouble();

        assertEquals("Conversion did not match", value, to);

        // make sure the buffer is rewound
        from.toDouble();
    }

    @Test
    public void testDoubleObject() {
        Double value = Double.MAX_VALUE;
        Bytes from = Bytes.fromDouble(value);
        Double to = from.toDouble(null);

        assertEquals("Conversion did not match", value, to);
    }

    @Test
    public void testDoubleObjectNull() {
        Double value = null;
        Bytes from = Bytes.fromDouble(value);
        Double to = from.toDouble(null);

        assertEquals("Conversion did not match", value, to);
    }

    @Test
    public void testUuid() {
        UUID value = UUID.randomUUID();
        Bytes from = Bytes.fromUuid(value);
        UUID to = from.toUuid();

        assertEquals("Conversion did not match", value, to);

        // make sure the buffer is rewound
        from.toUuid();
    }

    @Test
    public void testUuidNull() {
        UUID value = null;
        Bytes from = Bytes.fromUuid(value);
        UUID to = from.toUuid();

        assertEquals("Conversion did not match", value, to);;
    }

    @Test
    public void testUuidString() {
        String value = UUID.randomUUID().toString();
        Bytes from = Bytes.fromUuid(value);
        UUID to = from.toUuid();

        assertEquals("Conversion did not match", value, to.toString());
    }

    @Test
    public void testUuidLongs() {
        long msb = 1l;
        long lsb = 2l;
        Bytes from = Bytes.fromUuid(msb, lsb);
        UUID to = from.toUuid();

        assertEquals("Conversion did not match", msb, to.getMostSignificantBits());
        assertEquals("Conversion did not match", lsb, to.getLeastSignificantBits());
    }

    @Test
    public void testTimeUuid() {
        com.eaio.uuid.UUID value = new com.eaio.uuid.UUID();
        Bytes from = Bytes.fromTimeUuid(value);
        com.eaio.uuid.UUID to = from.toTimeUuid();

        assertEquals("Conversion did not match", value, to);

        // make sure the buffer is rewound
        from.toTimeUuid();
    }

    @Test
    public void testTimeUuidNull() {
        com.eaio.uuid.UUID value = null;
        Bytes from = Bytes.fromTimeUuid(value);
        com.eaio.uuid.UUID to = from.toTimeUuid();

        assertEquals("Conversion did not match", value, to);
    }

    @Test
    public void testTimeUuidString() {
        String value = new com.eaio.uuid.UUID().toString();
        Bytes from = Bytes.fromTimeUuid(value);
        com.eaio.uuid.UUID to = from.toTimeUuid();

        assertEquals("Conversion did not match", value, to.toString());
    }

    @Test
    public void testTimeUuidLongs() {
        long time = 1l;
        long clockSeq = 2l;
        Bytes from = Bytes.fromTimeUuid(time, clockSeq);
        com.eaio.uuid.UUID to = from.toTimeUuid();

        assertEquals("Conversion did not match", time, to.getTime());
        assertEquals("Conversion did not match", clockSeq, to.getClockSeqAndNode());
    }

    /**
     * The thrift API passes an array full of all kinds of stuff.  This test ensures that the Bytes class operates correctly
     * when this is the case (e.g. when read results from cassandra).
     */
    @Test
    public void testUTF8Buffer() {
        String string = "This is a test";

        ByteBuffer buffer = ByteBuffer.allocate(Bytes.SIZEOF_DOUBLE + string.length() + Bytes.SIZEOF_LONG);
        buffer.putDouble(Double.MAX_VALUE).put(string.getBytes(Bytes.UTF8)).putLong(Long.MAX_VALUE);

        Bytes from = Bytes.fromByteBuffer(
                ByteBuffer.wrap(buffer.array(), Bytes.SIZEOF_DOUBLE, string.length())
        );
        String to = from.toUTF8();

        assertEquals("Conversion did not match", string, to);

        // make sure the buffer is rewound
        to = from.toUTF8();

        assertEquals("Conversion did not match", string, to);
    }

    @Test
    public void testLength() {
        ByteBuffer buffer = ByteBuffer.allocate(Bytes.SIZEOF_DOUBLE + Bytes.SIZEOF_INT + Bytes.SIZEOF_LONG);
        buffer.putDouble(Double.MAX_VALUE).putInt(Integer.MIN_VALUE).putLong(Long.MAX_VALUE);

        Bytes bytes = Bytes.fromByteBuffer(ByteBuffer.wrap(buffer.array(), Bytes.SIZEOF_DOUBLE, Bytes.SIZEOF_INT));

        assertEquals("The length method didn't return the correct value", Bytes.SIZEOF_INT, bytes.length());
    }

    @Test
    public void testNulls() {
        Bytes bytes = Bytes.NULL;
        assertNull(bytes.toBoolean(null));
        assertNull(bytes.toByte(null));
        assertNull(bytes.toChar(null));
        assertNull(bytes.toDouble(null));
        assertNull(bytes.toFloat(null));
        assertNull(bytes.toInt(null));
        assertNull(bytes.toShort(null));
        assertNull(bytes.toLong(null));
    }

    @Test
    public void testDuplicate() {
        Bytes bytes = Bytes.NULL;
        assertEquals("Duplicate not equal", Bytes.NULL, bytes.duplicate());

        bytes = Bytes.fromUTF8("some  bytes");
        assertEquals("Duplicate not equal", bytes, bytes.duplicate());

        String string = "some string";
        ByteBuffer buffer = ByteBuffer.allocate(Bytes.SIZEOF_DOUBLE + string.length() + Bytes.SIZEOF_LONG);
        buffer.putDouble(Double.MAX_VALUE).put(string.getBytes(Bytes.UTF8)).putLong(Long.MAX_VALUE);

        bytes = Bytes.fromByteBuffer(
                ByteBuffer.wrap(buffer.array(), Bytes.SIZEOF_DOUBLE, string.length())
        );
        assertEquals("Duplicate not equal", bytes, bytes.duplicate());
    }

    @Test
    public void testBuilder() {
        final String utf8Part = "foo";
        final int intPart = 123;
        Bytes bytes = Bytes.builder().addUTF8(utf8Part).addInt(intPart).build();

        final ByteBuffer byteBuffer = bytes.getBytes();

        final byte[] target = new byte[3];
        byteBuffer.get(target, 0, 3);
        String utf8PartActual = new String(target, Bytes.UTF8);

        int intPartActual = byteBuffer.getInt();

        assertEquals("String part did not match", utf8Part, utf8PartActual);
        assertEquals("Int part did not match", intPart, intPartActual);

        // todo: test all the different types
    }
}
