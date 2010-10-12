package org.scale7.cassandra.pelops;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;

import java.util.Arrays;
import java.util.UUID;

import org.junit.Test;
import org.scale7.cassandra.pelops.serializable.SerializableObject;

/**
 * Tests the {@link Bytes} class.
 */
public class BytesUnitTest {
    @Test
    public void testNullArray() {
        Bytes bytes = new Bytes(null);
        assertNull("The underlying array should not be null", bytes.getBytes());
    }
    
    @Test
    public void testEquals() {
        Bytes one = Bytes.fromBytes(new byte[] {1, 2, 3, 4, 5});
        Bytes two = Bytes.fromBytes(new byte[] {1, 2, 3, 4, 5});

        assertTrue("Two instance were not equal", one.equals(two));
    }

    @Test
    public void testEqualsNegative() {
        Bytes one = Bytes.fromBytes(new byte[] {5, 4, 3, 2, 1});
        Bytes two = Bytes.fromBytes(new byte[] {1, 2, 3, 4, 5});

        assertFalse("Two instance were not equal", one.equals(two));
    }

    @Test
    public void testHashCode() {
        Bytes one = Bytes.fromBytes(new byte[] {1, 2, 3, 4, 5});
        Bytes two = Bytes.fromBytes(new byte[] {1, 2, 3, 4, 5});

        assertEquals("The hashCode method should generate the same value from the same input", one.hashCode(), two.hashCode());
    }

    @Test
    public void testHashCodeDifferent() {
        Bytes one = Bytes.fromBytes(new byte[] {5, 4, 3, 2, 1});
        Bytes two = Bytes.fromBytes(new byte[] {1, 2, 3, 4, 5});

        assertFalse("The hashCode method should generate the a different value from different input", one.hashCode() == two.hashCode());
    }

    @Test
    public void testBytes() {
        byte[] value = {1, 2, 3, 4, 5};
        Bytes from = Bytes.fromBytes(value);
        byte[] to = from.getBytes();

        assertTrue("Conversion did not match", Arrays.equals(value, to));
    }

    @Test
    public void testByte() {
        byte value = (byte) 9;
        Bytes from = Bytes.fromByte(value);
        byte to = from.toByte();

        assertEquals("Conversion did not match", value, to);
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
    }

    @Test
    public void testBooleanFalse() {
        boolean value = false;

        Bytes from = Bytes.fromBoolean(value);
        boolean to = from.toBoolean();

        assertEquals("Conversion did not match", value, to);
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
    }

    @Test
    public void testUuidNull() {
        UUID value = null;
        Bytes from = Bytes.fromUuid(value);
        UUID to = from.toUuid();

        assertEquals("Conversion did not match", value, to);
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
   
    
 
    
    
   

    @Test
    public void testNulls() {
        Bytes bytes = new Bytes(null);
        assertNull(bytes.toBoolean(null));
        assertNull(bytes.toByte(null));
        assertNull(bytes.toChar(null));
        assertNull(bytes.toDouble(null));
        assertNull(bytes.toFloat(null));
        assertNull(bytes.toInt(null));
        assertNull(bytes.toShort(null));
        assertNull(bytes.toLong(null));
    }
}
