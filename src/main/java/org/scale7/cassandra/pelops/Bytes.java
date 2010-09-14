package org.scale7.cassandra.pelops;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

/**
 * Wraps a byte[] and provides useful methods to operate on it.  Also provides numerous factory methods to create instances
 * based on various data types.
 *
 * <p>In an effort to provide a very stable and well tested marshalling strategy
 * this class uses the various methods available on {@link java.nio.ByteBuffer} to perform serialization.  The exceptions
 * to this are the UUID and String methods (see their javadoc comments for details).</p>
 *
 * <p>The down side of the marshalling strategy used in this class this class is a LOT of ByteBuffer instances being
 * created.  If you think this will be an issue then consider avoiding the {@link #fromBoolean(boolean) various}
 * {@link #toBoolean() helper} {@link #fromTimeUuid(com.eaio.uuid.UUID) methods} and instead use the
 * {@link #fromBytes(byte[])} and {@link #getBytes()} methods directly.</p>
 */
public class Bytes {
    public static final Bytes EMPTY = new Bytes(new byte[0]);
    public static final Bytes NULL = new Bytes(null);

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private static final int SIZEOF_BYTE = Byte.SIZE / Byte.SIZE;

    private static final int SIZEOF_BOOLEAN = SIZEOF_BYTE;
    private static final byte BOOLEAN_TRUE = (byte) 1;
    private static final byte BOOLEAN_FALSE = (byte) 0;

    private static final int SIZEOF_CHAR = Character.SIZE / Byte.SIZE;

    private static final int SIZEOF_SHORT = Short.SIZE / Byte.SIZE;
    private static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;
    private static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;

    private static final int SIZEOF_FLOAT = Float.SIZE / Byte.SIZE;
    private static final int SIZEOF_DOUBLE = Double.SIZE / Byte.SIZE;

    private static final int SIZEOF_UUID = SIZEOF_LONG + SIZEOF_LONG;

    private final byte[] bytes;
    private int hashCode = -1;

    /**
     * Constructs a new instance based on the provided byte array.
     * @param bytes the bytes
     */
    public Bytes(byte[] bytes) {
        this.bytes = bytes;

    }

    /**
     * Returns a string representation of the bytes as defined by the {@link java.util.Arrays#toString(byte[])} method.
     * <p><b>NOTE</b>: The {@link #toUTF8()} method provides the reverse value of the {@link #fromUTF8(String)} method.
     *
     * @return the string representation
     */
    @Override
    public String toString() {
        return Arrays.toString(this.bytes);
    }

    /**
     * The raw byte array.
     * @return the raw byte array
     */
    public byte[] getBytes() {
        return bytes;
    }

    /**
     * Determines if two instances of this class are equals using the {@link Arrays#equals(byte[], byte[])} method.
     * @param o the other instance
     * @return true if they are equal, otherwise false
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Bytes)) return false;

        Bytes byteArray = (Bytes) o;

        return Arrays.equals(bytes, byteArray.bytes);

    }

    /**
     * Calculates the hash code using {@link java.util.Arrays#hashCode(byte[])}.
     * <p>Note that the instances hashCode is calculated the first time this method is called and then cached.</p>
     * @return the hash code
     */
    @Override
    public int hashCode() {
        if (hashCode == -1) // only calculate the hash code once
            hashCode = Arrays.hashCode(bytes);

        return hashCode;
    }

    /**
     * Returns the length of the underlying byte array.
     * @return the length
     */
    public int length() {
        return this.bytes.length;
    }

    /**
     * Creates an instance based on the provided byte array.  The major difference from the {@link Bytes#Bytes(byte[])}
     * method is that this will return null if null is provided.
     * @param value the value
     * @return the Bytes instance or null (if null is provided)
     */
    public static Bytes fromBytes(byte[] value) {
        return new Bytes(value);
    }

    /**
     * Creates an instance based on the provided value.
     * @param value the value
     * @return the instance
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromChar(char value) {
        return new Bytes(ByteBuffer.allocate(SIZEOF_CHAR).putChar(value).array());
    }

    /**
     * Creates an instance based on the provided value handling nulls.
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromChar(Character value) {
        return value == null ? new Bytes(null) : fromChar(value.charValue());
    }

    /**
     * Creates an instance based on the provided value.
     * @param value the value
     * @return the instance
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromByte(byte value) {
        return new Bytes(ByteBuffer.allocate(SIZEOF_BYTE).put(value).array());
    }

    /**
     * Creates an instance based on the provided value handling nulls.
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromByte(Byte value) {
        return value == null ? new Bytes(null) : fromByte(value.byteValue());
    }

    /**
     * Creates an instance based on the provided value.
     * @param value the value
     * @return the instance
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromLong(long value) {
        return new Bytes(ByteBuffer.allocate(SIZEOF_LONG).putLong(value).array());
    }

    /**
     * Creates an instance based on the provided value handling nulls.
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromLong(Long value) {
        return value == null ? new Bytes(null) : fromLong(value.longValue());
    }

    /**
     * Creates an instance based on the provided value.
     * @param value the value
     * @return the instance
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromInt(int value) {
        return new Bytes(ByteBuffer.allocate(SIZEOF_INT).putInt(value).array());
    }

    /**
     * Creates an instance based on the provided value handling nulls.
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromInt(Integer value) {
        return value == null ? new Bytes(null) : fromInt(value.intValue());
    }

    /**
     * Creates an instance based on the provided value.
     * @param value the value
     * @return the instance
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromShort(short value) {
        return new Bytes(ByteBuffer.allocate(SIZEOF_SHORT).putShort(value).array());
    }

    /**
     * Creates an instance based on the provided value handling nulls.
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromShort(Short value) {
        return value == null ? new Bytes(null) : fromShort(value.shortValue());
    }

    /**
     * Creates an instance based on the provided value.
     * @param value the value
     * @return the instance
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromDouble(double value) {
        return new Bytes(ByteBuffer.allocate(SIZEOF_DOUBLE).putDouble(value).array());
    }

    /**
     * Creates an instance based on the provided value handling nulls.
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromDouble(Double value) {
        return value == null ? new Bytes(null) : fromDouble(value.doubleValue());
    }

    /**
     * Creates an instance based on the provided value.
     * @param value the value
     * @return the instance
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromFloat(float value) {
        return new Bytes(ByteBuffer.allocate(SIZEOF_FLOAT).putFloat(value).array());
    }

    /**
     * Creates an instance based on the provided value handling nulls.
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromFloat(Float value) {
        return value == null ? new Bytes(null) : fromFloat(value.floatValue());
    }

    /**
     * Creates an instance based on the provided value.
     * @param value the value
     * @return the instance
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromBoolean(boolean value) {
        return new Bytes(ByteBuffer.allocate(SIZEOF_BOOLEAN).put(value ? BOOLEAN_TRUE : BOOLEAN_FALSE).array());
    }

    /**
     * Creates an instance based on the provided value handling nulls.
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromBoolean(Boolean value) {
        return value == null ? new Bytes(null) : fromBoolean(value.booleanValue());
    }

    /**
     * Creates an instance based on the provided value handling nulls.  If the provided value is not null then this method
     * delegates to {@link #fromUuid(long, long)} to perform the deserialization.
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on long serializaion format
     */
    public static Bytes fromUuid(UUID value) {
        return value == null ? new Bytes(null) : fromUuid(value.getMostSignificantBits(), value.getLeastSignificantBits());
    }

    /**
     * Creates an instance based on the provided value handling nulls.  If the provided value is not null then a new
     * instance of {@link UUID} is created using {@link java.util.UUID#fromString(String)} and then
     * {@link #fromUuid(long, long)} is called to perform the deserialization.
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on long serializaion format
     */
    public static Bytes fromUuid(String value) {
        return value == null ? new Bytes(null) : fromUuid(UUID.fromString(value));
    }

    /**
     * Creates an instance based on the provided values.  The {@link java.nio.ByteBuffer#putLong(long)} is called twice,
     * the first call for the msb value and second for the lsb value.
     * @param msb the msb value
     * @param lsb the lsb value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on long serializaion format
     */
    public static Bytes fromUuid(long msb, long lsb) {
        return new Bytes(
                ByteBuffer.allocate(SIZEOF_UUID)
                        .putLong(msb)
                        .putLong(lsb).array()
        );
    }

    /**
     * Creates an instance based on the provided value handling nulls.  If the provided value is not null then this method
     * delegates to {@link #fromTimeUuid(long, long)} to perform the deserialization.
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on long serializaion format
     */
    public static Bytes fromTimeUuid(com.eaio.uuid.UUID value) {
        return value == null ? new Bytes(null) : fromTimeUuid(value.getTime(), value.getClockSeqAndNode());
    }

    /**
     * Creates an instance based on the provided value handling nulls.  If the provided value is not null then a new
     * instance of {@link com.eaio.uuid.UUID} is created and then
     * {@link #fromTimeUuid(long, long)} is called to perform the deserialization.
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on long serializaion format
     */
    public static Bytes fromTimeUuid(String value) {
        return value == null ? new Bytes(null) : fromTimeUuid(new com.eaio.uuid.UUID(value));
    }

    /**
     * Creates an instance based on the provided values.  The {@link java.nio.ByteBuffer#putLong(long)} is called twice,
     * the first call for the time value and second for the clockSeqAndNode value.
     * @param time the time value
     * @param clockSeqAndNode the clockSeqAndNode value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on long serializaion format
     */
    public static Bytes fromTimeUuid(long time, long clockSeqAndNode) {
        return new Bytes(
                ByteBuffer.allocate(SIZEOF_UUID)
                        .putLong(time)
                        .putLong(clockSeqAndNode).array()
        );
    }

    /**
     * Creates an instance based on the provided value in UTF-8 format handling nulls.
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see String#getBytes(java.nio.charset.Charset) for details on the format
     */
    public static Bytes fromUTF8(String value) {
        return value == null ? new Bytes(null) : new Bytes(value.getBytes(UTF8));
    }


    /**
     * Converts the backing array to the appropriate object instance handling nulls.
     * @param defaultIfNull the value to return is the backing array is null or empty
     * @return the deserialized instance
     */
    public Character toChar(Character defaultIfNull) {
        if (isNull())
            return defaultIfNull;
        else
            return toChar();
    }

    /**
     * Converts the backing array to the appropriate primitive.
     * @return the deserialized primitive
     * @throws IllegalStateException if the underlying array does not contain the appropriate data
     */
    public char toChar() throws IllegalStateException {
        try {
            return ByteBuffer.wrap(this.bytes).getChar();
        } catch (BufferUnderflowException e) {
            throw new IllegalStateException("Failed to read value due to invalid format.  See cause for details...", e);
        }
    }

    /**
     * Converts the backing array to the appropriate object instance handling nulls.
     * @param defaultIfNull the value to return is the backing array is null or empty
     * @return the deserialized instance
     */
    public Byte toByte(Byte defaultIfNull) {
        if (isNull())
            return defaultIfNull;
        else
            return toByte();
    }

    /**
     * Converts the backing array to the appropriate primitive.
     * @return the deserialized primitive
     * @throws IllegalStateException if the underlying array does not contain the appropriate data
     */
    public byte toByte() throws IllegalStateException {
        try {
            // seems a bit excessive to wrap for one byte...
            return ByteBuffer.wrap(this.bytes).get();
        } catch (BufferUnderflowException e) {
            throw new IllegalStateException("Failed to read value due to invalid format.  See cause for details...", e);
        }
    }

    /**
     * Converts the backing array to the appropriate object instance handling nulls.
     * @param defaultIfNull the value to return is the backing array is null or empty
     * @return the deserialized instance
     */
    public Long toLong(Long defaultIfNull) {
        if (isNull())
            return defaultIfNull;
        else
            return toLong();
    }

    /**
     * Converts the backing array to the appropriate primitive.
     * @return the deserialized primitive
     * @throws IllegalStateException if the underlying array does not contain the appropriate data
     */
    public long toLong() throws IllegalStateException {
        try {
            return ByteBuffer.wrap(this.bytes).getLong();
        } catch (BufferUnderflowException e) {
            throw new IllegalStateException("Failed to read value due to invalid format.  See cause for details...", e);
        }
    }

    /**
     * Converts the backing array to the appropriate object instance handling nulls.
     * @param defaultIfNull the value to return is the backing array is null or empty
     * @return the deserialized instance
     */
    public Integer toInt(Integer defaultIfNull) {
        if (isNull())
            return defaultIfNull;
        else
            return toInt();
    }

    /**
     * Converts the backing array to the appropriate primitive.
     * @return the deserialized primitive
     * @throws IllegalStateException if the underlying array does not contain the appropriate data
     */
    public int toInt() throws IllegalStateException {
        try {
            return ByteBuffer.wrap(this.bytes).getInt();
        } catch (BufferUnderflowException e) {
            throw new IllegalStateException("Failed to read value due to invalid format.  See cause for details...", e);
        }
    }

    /**
     * Converts the backing array to the appropriate object instance handling nulls.
     * @param defaultIfNull the value to return is the backing array is null or empty
     * @return the deserialized instance
     */
    public Short toShort(Short defaultIfNull) {
        if (isNull())
            return defaultIfNull;
        else
            return toShort();
    }

    /**
     * Converts the backing array to the appropriate primitive.
     * @return the deserialized primitive
     * @throws IllegalStateException if the underlying array does not contain the appropriate data
     */
    public short toShort() throws IllegalStateException {
        try {
            return ByteBuffer.wrap(this.bytes).getShort();
        } catch (BufferUnderflowException e) {
            throw new IllegalStateException("Failed to read value due to invalid format.  See cause for details...", e);
        }
    }

    /**
     * Converts the backing array to the appropriate object instance handling nulls.
     * @param defaultIfNull the value to return is the backing array is null or empty
     * @return the deserialized instance
     */
    public Double toDouble(Double defaultIfNull) {
        if (isNull())
            return defaultIfNull;
        else
            return toDouble();
    }

    /**
     * Converts the backing array to the appropriate primitive.
     * @return the deserialized primitive
     * @throws IllegalStateException if the underlying array does not contain the appropriate data
     */
    public double toDouble() throws IllegalStateException {
        try {
            return ByteBuffer.wrap(this.bytes).getDouble();
        } catch (BufferUnderflowException e) {
            throw new IllegalStateException("Failed to read value due to invalid format.  See cause for details...", e);
        }
    }

    /**
     * Converts the backing array to the appropriate object instance handling nulls.
     * @param defaultIfNull the value to return is the backing array is null or empty
     * @return the deserialized instance
     */
    public Float toFloat(Float defaultIfNull) {
        if (isNull())
            return defaultIfNull;
        else
            return toFloat();
    }

    /**
     * Converts the backing array to the appropriate primitive.
     * @return the deserialized primitive
     * @throws IllegalStateException if the underlying array does not contain the appropriate data
     */
    public float toFloat() throws IllegalStateException {
        try {
            return ByteBuffer.wrap(this.bytes).getFloat();
        } catch (BufferUnderflowException e) {
            throw new IllegalStateException("Failed to read value due to invalid format.  See cause for details...", e);
        }
    }

    /**
     * Converts the backing array to the appropriate object instance handling nulls.
     * @param defaultIfNull the value to return is the backing array is null or empty
     * @return the deserialized instance
     */
    public Boolean toBoolean(Boolean defaultIfNull) {
        if (isNull())
            return defaultIfNull;
        else
            return toBoolean();
    }

    /**
     * Converts the backing array to the appropriate primitive.
     * @return the deserialized primitive
     * @throws IllegalStateException if the underlying array does not contain the appropriate data
     */
    public boolean toBoolean() throws IllegalStateException {
        try {
            return ByteBuffer.wrap(this.bytes).get() != BOOLEAN_FALSE;
        } catch (BufferUnderflowException e) {
            throw new IllegalStateException("Failed to read value due to invalid format.  See cause for details...", e);
        }
    }

    /**
     * Converts the backing array to the appropriate object instance handling nulls.
     * @return the deserialized instance or null if the backing array was null or empty
     * @throws IllegalStateException if the underlying array does not contain the appropriate data
     */
    public UUID toUuid() throws IllegalStateException {
        if (isNull()) return null;

        ByteBuffer buffer = ByteBuffer.wrap(this.bytes);
        try {
            long msb = buffer.getLong();
            long lsb = buffer.getLong();

            return new UUID(msb, lsb);
        } catch (BufferUnderflowException e) {
            throw new IllegalStateException("Failed to read value due to invalid format.  See cause for details...", e);
        }
    }

    /**
     * Converts the backing array to the appropriate object instance handling nulls.
     * @return the deserialized instance or null if the backing array was null or empty
     * @throws IllegalStateException if the underlying array does not contain the appropriate data
     */
    public com.eaio.uuid.UUID toTimeUuid() throws IllegalStateException {
        if (isNull()) return null;

        ByteBuffer buffer = ByteBuffer.wrap(this.bytes);
        try {
            long time = buffer.getLong();
            long clockSeqAndNode = buffer.getLong();

            return new com.eaio.uuid.UUID(time, clockSeqAndNode);
        } catch (BufferUnderflowException e) {
            throw new IllegalStateException("Failed to read value due to invalid format.  See cause for details...", e);
        }
    }

    /**
     * Converts the backing array to the appropriate object instance handling nulls.
     * @return the deserialized instance or null if the backing array was null or empty
     */
    public String toUTF8() {
        return isNull() ? null : new String(this.bytes, UTF8);
    }

    /**
     * Transforms the provided list of {@link Bytes} instances into a list of byte arrays.
     * @param arrays the list of Bytes instances
     * @return the list of byte arrays
     */
    public static Set<byte[]> transformBytesToSet(Collection<Bytes> arrays) {
        if (arrays == null) return null;

        Set<byte[]> transformed = new HashSet<byte[]>(arrays.size());
        for (Bytes array : arrays) {
            transformed.add(array.getBytes());
        }

        return transformed;
    }

    /**
     * Transforms the provided list of {@link Bytes} instances into a list of byte arrays.
     * @param arrays the list of Bytes instances
     * @return the list of byte arrays
     */
    public static List<byte[]> transformBytesToList(Collection<Bytes> arrays) {
        if (arrays == null) return null;

        List<byte[]> transformed = new ArrayList<byte[]>(arrays.size());
        for (Bytes array : arrays) {
            transformed.add(array.getBytes());
        }

        return transformed;
    }

    /**
     * Transforms the provided list of {@link String} instances into a list of byte arrays.
     * @param strings the list of Bytes instances
     * @return the list of byte arrays
     */
    public static Set<byte[]> transformUTF8ToSet(Collection<String> strings) {
        if (strings == null) return null;

        Set<byte[]> transformed = new HashSet<byte[]>(strings.size());
        for (String string : strings) {
            transformed.add(Bytes.fromUTF8(string).getBytes());
        }

        return transformed;
    }

    /**
     * Transforms the provided list of {@link String} instances into a list of byte arrays.
     * @param strings the list of Bytes instances
     * @return the list of byte arrays
     */
    public static List<byte[]> transformUTF8ToList(Collection<String> strings) {
        if (strings == null) return null;

        List<byte[]> transformed = new ArrayList<byte[]>(strings.size());
        for (String string : strings) {
            transformed.add(Bytes.fromUTF8(string).getBytes());
        }

        return transformed;
    }

    /**
     * Returns the underlying byte array of the provided bytes instance or null if the provided instance was null.
     * @param bytes the bytes instance
     * @return the underlying byte array of the instance or null
     */
    public static byte[] nullSafeGet(Bytes bytes) {
        return bytes == null ? null : bytes.getBytes();
    }

    public boolean isNull() {
        return this.bytes == null;
    }
}