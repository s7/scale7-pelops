package org.scale7.cassandra.pelops;

import java.nio.Buffer;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

/**
 * Wraps a {@link java.nio.ByteBuffer} and provides useful methods to operate on it.
 * Also provides numerous factory methods to create instances based on various data types.
 * <p/>
 * <p>In an effort to provide a very stable and well tested marshalling strategy
 * this class uses the various methods available on {@link java.nio.ByteBuffer} to perform serialization.  The exceptions
 * to this are the UUID and String methods (see their javadoc comments for details).</p>
 * <p/>
 * <b>Note</b>: Instances of this class should *not* be considered thread safe.
 */
public class Bytes {
    public static final Bytes EMPTY = new Bytes(new byte[0]);
    public static final Bytes NULL = new Bytes((ByteBuffer) null);

    static final int SIZEOF_BYTE = Byte.SIZE / Byte.SIZE;

    static final int SIZEOF_BOOLEAN = SIZEOF_BYTE;

    static final byte BOOLEAN_TRUE = (byte) 1;
    static final byte BOOLEAN_FALSE = (byte) 0;
    static final int SIZEOF_CHAR = Character.SIZE / Byte.SIZE;

    static final int SIZEOF_SHORT = Short.SIZE / Byte.SIZE;

    static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;
    static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;
    static final int SIZEOF_FLOAT = Float.SIZE / Byte.SIZE;

    static final int SIZEOF_DOUBLE = Double.SIZE / Byte.SIZE;
    static final int SIZEOF_UUID = SIZEOF_LONG + SIZEOF_LONG;

    static final Charset UTF8 = Charset.forName("UTF-8");

    private final ByteBuffer bytes;
    private int hashCode = -1;

    /**
     * Constructs a new instance based on the provided byte array.
     *
     * @param bytes the bytes
     */
    public Bytes(byte[] bytes) {
        this(ByteBuffer.wrap(bytes));
    }

    /**
     * Constructs a new instance based on the provided byte buffer.  The {@link java.nio.ByteBuffer#position()} must be
     * in the correct position to read the appropriate value from it.
     *
     * @param bytes the bytes
     */
    public Bytes(ByteBuffer bytes) {
        if (bytes != null) {
            this.bytes = bytes.duplicate();
            this.bytes.mark();
        } else
            this.bytes = null;
    }

    /**
     * Constructs a new instance based on the provided buffer (must be an instance of ByteBuffer).
     *
     * @param bytes the bytes
     */
    private Bytes(Buffer bytes) {
        this((ByteBuffer) bytes);
    }

    /**
     * Returns an (expensive) string representation of the bytes as defined by the {@link java.util.Arrays#toString(byte[])} method.
     * <p><b>NOTE</b>: The {@link #toUTF8()} method provides the reverse value of the {@link #fromUTF8(String)} method.
     *
     * @return the string representation
     */
    @Override
    public String toString() {
        if (isNull()) return null;

        bytes.reset();
        return Arrays.toString(Arrays.copyOfRange(bytes.array(), bytes.position(), bytes.limit()));
    }

    /**
     * Returns the underlying {@link ByteBuffer}.
     *
     * @return the raw byte array
     */
    public ByteBuffer getBytes() {
        return bytes;
    }

    /**
     * Returns a duplicate of the bytes instance.  The underlying {@link java.nio.ByteBuffer} is duplicated using the
     * {@link java.nio.ByteBuffer#duplicate()} method.
     *
     * @return the raw byte array
     * @see java.nio.ByteBuffer#duplicate()
     */
    public Bytes duplicate() {
        return isNull() ? Bytes.NULL : new Bytes(bytes.duplicate());
    }

    /**
     * Determines if two instances of this class are equals using the {@link Arrays#equals(byte[], byte[])} method.
     *
     * @param o the other instance
     * @return true if they are equal, otherwise false
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Bytes)) return false;

        Bytes bytes = (Bytes) o;

        return this.bytes.equals(bytes.bytes);
    }

    /**
     * Calculates the hash code using {@link java.util.Arrays#hashCode(byte[])}.
     * <p>Note that the instances hashCode is calculated the first time this method is called and then cached.</p>
     *
     * @return the hash code
     */
    @Override
    public int hashCode() {
        if (hashCode == -1) // only calculate the hash code once
            hashCode = this.bytes.hashCode();

        return hashCode;
    }

    /**
     * Returns the length of the value within the underlying byte buffer.
     *
     * @return the length
     */
    public int length() {
        return this.bytes.remaining();
    }

    /**
     * Creates an instance based on the provided byte array.
     *
     * @param value the value
     * @return the Bytes instance
     * @deprecated use {@link #fromByteArray(byte[])} instead
     */
    @Deprecated
    public static Bytes fromBytes(byte[] value) {
        return fromByteArray(value);
    }

    /**
     * Creates an instance based on the provided byte array.
     *
     * @param value the value
     * @return the Bytes instance
     */
    public static Bytes fromByteArray(byte[] value) {
        return new Bytes(value);
    }

    /**
     * Creates an instance based on the provided byte buffer.
     *
     * @param value the value
     * @return the Bytes instance
     */
    public static Bytes fromByteBuffer(ByteBuffer value) {
        return new Bytes(value);
    }

    /**
     * Creates an instance based on the provided value.
     *
     * @param value the value
     * @return the instance
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromChar(char value) {
        return new Bytes(ByteBuffer.allocate(SIZEOF_CHAR).putChar(value).rewind());
    }

    /**
     * Creates an instance based on the provided value handling nulls.
     *
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromChar(Character value) {
        return value == null ? NULL : fromChar(value.charValue());
    }

    /**
     * Creates an instance based on the provided value.
     *
     * @param value the value
     * @return the instance
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromByte(byte value) {
        return new Bytes(ByteBuffer.allocate(SIZEOF_BYTE).put(value).rewind());
    }

    /**
     * Creates an instance based on the provided value handling nulls.
     *
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromByte(Byte value) {
        return value == null ? NULL : fromByte(value.byteValue());
    }

    /**
     * Creates an instance based on the provided value.
     *
     * @param value the value
     * @return the instance
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromLong(long value) {
        return new Bytes(ByteBuffer.allocate(SIZEOF_LONG).putLong(value).rewind());
    }

    /**
     * Creates an instance based on the provided value handling nulls.
     *
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromLong(Long value) {
        return value == null ? NULL : fromLong(value.longValue());
    }

    /**
     * Creates an instance based on the provided value.
     *
     * @param value the value
     * @return the instance
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromInt(int value) {
        return new Bytes(ByteBuffer.allocate(SIZEOF_INT).putInt(value).rewind());
    }

    /**
     * Creates an instance based on the provided value handling nulls.
     *
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromInt(Integer value) {
        return value == null ? NULL : fromInt(value.intValue());
    }

    /**
     * Creates an instance based on the provided value.
     *
     * @param value the value
     * @return the instance
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromShort(short value) {
        return new Bytes(ByteBuffer.allocate(SIZEOF_SHORT).putShort(value).rewind());
    }

    /**
     * Creates an instance based on the provided value handling nulls.
     *
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromShort(Short value) {
        return value == null ? NULL : fromShort(value.shortValue());
    }

    /**
     * Creates an instance based on the provided value.
     *
     * @param value the value
     * @return the instance
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromDouble(double value) {
        return new Bytes(ByteBuffer.allocate(SIZEOF_DOUBLE).putDouble(value).rewind());
    }

    /**
     * Creates an instance based on the provided value handling nulls.
     *
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromDouble(Double value) {
        return value == null ? NULL : fromDouble(value.doubleValue());
    }

    /**
     * Creates an instance based on the provided value.
     *
     * @param value the value
     * @return the instance
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromFloat(float value) {
        return new Bytes(ByteBuffer.allocate(SIZEOF_FLOAT).putFloat(value).rewind());
    }

    /**
     * Creates an instance based on the provided value handling nulls.
     *
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromFloat(Float value) {
        return value == null ? NULL : fromFloat(value.floatValue());
    }

    /**
     * Creates an instance based on the provided value.
     *
     * @param value the value
     * @return the instance
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromBoolean(boolean value) {
        return new Bytes(ByteBuffer.allocate(SIZEOF_BOOLEAN).put(value ? BOOLEAN_TRUE : BOOLEAN_FALSE).rewind());
    }

    /**
     * Creates an instance based on the provided value handling nulls.
     *
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromBoolean(Boolean value) {
        return value == null ? NULL : fromBoolean(value.booleanValue());
    }

    /**
     * Creates an instance based on the provided value handling nulls.  If the provided value is not null then this method
     * delegates to {@link #fromUuid(long, long)} to perform the deserialization.
     *
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on long serializaion format
     */
    public static Bytes fromUuid(UUID value) {
        return value == null ? NULL : fromUuid(value.getMostSignificantBits(), value.getLeastSignificantBits());
    }

    /**
     * Creates an instance based on the provided value handling nulls.  If the provided value is not null then a new
     * instance of {@link UUID} is created using {@link java.util.UUID#fromString(String)} and then
     * {@link #fromUuid(long, long)} is called to perform the deserialization.
     *
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on long serializaion format
     */
    public static Bytes fromUuid(String value) {
        return value == null ? NULL : fromUuid(UUID.fromString(value));
    }

    /**
     * Creates an instance based on the provided values.  The {@link java.nio.ByteBuffer#putLong(long)} is called twice,
     * the first call for the msb value and second for the lsb value.
     *
     * @param msb the msb value
     * @param lsb the lsb value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on long serializaion format
     */
    public static Bytes fromUuid(long msb, long lsb) {
        return new Bytes(
                ByteBuffer.allocate(SIZEOF_UUID).putLong(msb).putLong(lsb).rewind()
        );
    }

    /**
     * Creates an instance based on the provided value handling nulls.  If the provided value is not null then this method
     * delegates to {@link #fromTimeUuid(long, long)} to perform the deserialization.
     *
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on long serializaion format
     */
    public static Bytes fromTimeUuid(com.eaio.uuid.UUID value) {
        return value == null ? NULL : fromTimeUuid(value.getTime(), value.getClockSeqAndNode());
    }

    /**
     * Creates an instance based on the provided value handling nulls.  If the provided value is not null then this method
     * delegates to {@link #fromTimeUuid(long, long)} to perform the deserialization.
     *
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on long serializaion format
     */
    public static Bytes fromTimeUuid(UUID value) {
        return value == null ? NULL : fromTimeUuid(value.toString());
    }

    /**
     * Creates an instance based on the provided value handling nulls.  If the provided value is not null then a new
     * instance of {@link com.eaio.uuid.UUID} is created and then
     * {@link #fromTimeUuid(long, long)} is called to perform the deserialization.
     *
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on long serializaion format
     */
    public static Bytes fromTimeUuid(String value) {
        return value == null ? NULL : fromTimeUuid(new com.eaio.uuid.UUID(value));
    }

    /**
     * Creates an instance based on the provided values.  The {@link java.nio.ByteBuffer#putLong(long)} is called twice,
     * the first call for the time value and second for the clockSeqAndNode value.
     *
     * @param time            the time value
     * @param clockSeqAndNode the clockSeqAndNode value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on long serializaion format
     */
    public static Bytes fromTimeUuid(long time, long clockSeqAndNode) {
        return new Bytes(
                ByteBuffer.allocate(SIZEOF_UUID).putLong(time).putLong(clockSeqAndNode).rewind()
        );
    }

    /**
     * Creates an instance based on the provided value in UTF-8 format handling nulls.
     *
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see String#getBytes(java.nio.charset.Charset) for details on the format
     */
    public static Bytes fromUTF8(String value) {
        return value == null ? NULL : new Bytes(value.getBytes(UTF8));
    }


    /**
     * Converts the backing array to the appropriate object instance handling nulls.
     *
     * @param defaultIfNull the value to return is the backing array is null
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
     *
     * @return the deserialized primitive
     * @throws IllegalStateException if the underlying array does not contain the appropriate data
     */
    public char toChar() throws IllegalStateException {
        try {
            bytes.reset();
            return this.bytes.getChar();
        } catch (BufferUnderflowException e) {
            throw new IllegalStateException("Failed to read value due to invalid format.  See cause for details...", e);
        } finally {
            this.bytes.reset();
        }
    }

    /**
     * Converts the backing array to the appropriate object instance handling nulls.
     *
     * @param defaultIfNull the value to return is the backing array is null
     * @return the deserialized instance
     */
    public byte[] toByteArray(byte[] defaultIfNull) {
        if (isNull())
            return defaultIfNull;
        else
            return toByteArray();
    }

    /**
     * Converts the backing array to the appropriate primitive.
     *
     * @return the deserialized primitive
     * @throws IllegalStateException if the underlying array does not contain the appropriate data
     */
    public byte[] toByteArray() throws IllegalStateException {
        try {
            bytes.reset();
            return Arrays.copyOfRange(this.bytes.array(), this.bytes.position(), this.bytes.limit());
        } catch (BufferUnderflowException e) {
            throw new IllegalStateException("Failed to read value due to invalid format.  See cause for details...", e);
        } finally {
            this.bytes.reset();
        }
    }

    /**
     * Converts the backing array to the appropriate object instance handling nulls.
     *
     * @param defaultIfNull the value to return is the backing array is null
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
     *
     * @return the deserialized primitive
     * @throws IllegalStateException if the underlying array does not contain the appropriate data
     */
    public byte toByte() throws IllegalStateException {
        try {
            bytes.reset();
            return this.bytes.get();
        } catch (BufferUnderflowException e) {
            throw new IllegalStateException("Failed to read value due to invalid format.  See cause for details...", e);
        } finally {
            this.bytes.reset();
        }
    }

    /**
     * Converts the backing array to the appropriate object instance handling nulls.
     *
     * @param defaultIfNull the value to return is the backing array is null
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
     *
     * @return the deserialized primitive
     * @throws IllegalStateException if the underlying array does not contain the appropriate data
     */
    public long toLong() throws IllegalStateException {
        try {
            bytes.reset();
            return this.bytes.getLong();
        } catch (BufferUnderflowException e) {
            throw new IllegalStateException("Failed to read value due to invalid format.  See cause for details...", e);
        } finally {
            this.bytes.reset();
        }
    }

    /**
     * Converts the backing array to the appropriate object instance handling nulls.
     *
     * @param defaultIfNull the value to return is the backing array is null
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
     *
     * @return the deserialized primitive
     * @throws IllegalStateException if the underlying array does not contain the appropriate data
     */
    public int toInt() throws IllegalStateException {
        try {
            bytes.reset();
            return this.bytes.getInt();
        } catch (BufferUnderflowException e) {
            throw new IllegalStateException("Failed to read value due to invalid format.  See cause for details...", e);
        } finally {
            this.bytes.reset();
        }
    }

    /**
     * Converts the backing array to the appropriate object instance handling nulls.
     *
     * @param defaultIfNull the value to return is the backing array is null
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
     *
     * @return the deserialized primitive
     * @throws IllegalStateException if the underlying array does not contain the appropriate data
     */
    public short toShort() throws IllegalStateException {
        try {
            bytes.reset();
            return this.bytes.getShort();
        } catch (BufferUnderflowException e) {
            throw new IllegalStateException("Failed to read value due to invalid format.  See cause for details...", e);
        } finally {
            this.bytes.reset();
        }
    }

    /**
     * Converts the backing array to the appropriate object instance handling nulls.
     *
     * @param defaultIfNull the value to return is the backing array is null
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
     *
     * @return the deserialized primitive
     * @throws IllegalStateException if the underlying array does not contain the appropriate data
     */
    public double toDouble() throws IllegalStateException {
        try {
            bytes.reset();
            return this.bytes.getDouble();
        } catch (BufferUnderflowException e) {
            throw new IllegalStateException("Failed to read value due to invalid format.  See cause for details...", e);
        } finally {
            this.bytes.reset();
        }
    }

    /**
     * Converts the backing array to the appropriate object instance handling nulls.
     *
     * @param defaultIfNull the value to return is the backing array is null
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
     *
     * @return the deserialized primitive
     * @throws IllegalStateException if the underlying array does not contain the appropriate data
     */
    public float toFloat() throws IllegalStateException {
        try {
            bytes.reset();
            return this.bytes.getFloat();
        } catch (BufferUnderflowException e) {
            throw new IllegalStateException("Failed to read value due to invalid format.  See cause for details...", e);
        } finally {
            this.bytes.reset();
        }
    }

    /**
     * Converts the backing array to the appropriate object instance handling nulls.
     *
     * @param defaultIfNull the value to return is the backing array is null
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
     *
     * @return the deserialized primitive
     * @throws IllegalStateException if the underlying array does not contain the appropriate data
     */
    public boolean toBoolean() throws IllegalStateException {
        try {
            bytes.reset();
            return this.bytes.get() != BOOLEAN_FALSE;
        } catch (BufferUnderflowException e) {
            throw new IllegalStateException("Failed to read value due to invalid format.  See cause for details...", e);
        } finally {
            this.bytes.reset();
        }
    }

    /**
     * Converts the backing array to the appropriate object instance handling nulls.
     *
     * @return the deserialized instance or null if the backing array was null
     * @throws IllegalStateException if the underlying array does not contain the appropriate data
     */
    public UUID toUuid() throws IllegalStateException {
        if (isNull()) return null;

        try {
            bytes.reset();
            long msb = this.bytes.getLong();
            long lsb = this.bytes.getLong();

            return new UUID(msb, lsb);
        } catch (BufferUnderflowException e) {
            throw new IllegalStateException("Failed to read value due to invalid format.  See cause for details...", e);
        } finally {
            this.bytes.reset();
        }
    }

    /**
     * Converts the backing array to the appropriate object instance handling nulls.
     *
     * @return the deserialized instance or null if the backing array was null
     * @throws IllegalStateException if the underlying array does not contain the appropriate data
     */
    public com.eaio.uuid.UUID toTimeUuid() throws IllegalStateException {
        if (isNull()) return null;

        try {
            bytes.reset();
            long time = this.bytes.getLong();
            long clockSeqAndNode = this.bytes.getLong();

            return new com.eaio.uuid.UUID(time, clockSeqAndNode);
        } catch (BufferUnderflowException e) {
            throw new IllegalStateException("Failed to read value due to invalid format.  See cause for details...", e);
        } finally {
            this.bytes.reset();
        }
    }

    /**
     * Efficiently constructs a time uuid from a bytes array without creating any intermediate objects
     *
     * @param uuid The bytes representing the time uuid.
     * @return A time uuid object
     */
    public static java.util.UUID timeUuidFromBytes(byte[] uuid)
    {
            long msb = 0;
            long lsb = 0;
            assert uuid.length == 16;
            for (int i=0; i<8; i++)
                    msb = (msb << 8) | (uuid[i] & 0xff);
            for (int i=8; i<16; i++)
                    lsb = (lsb << 8) | (uuid[i] & 0xff);

            com.eaio.uuid.UUID u = new com.eaio.uuid.UUID(msb,lsb);

            return java.util.UUID.fromString(u.toString());
    }

    /**
     * Createa a UTF-8 representation of a time uuid from a bytes array
     * @param uuid The bytes representing the time uuid.
     * @return A string representation of the time uuid
     */
    public static String utf8TimeUuidFromBytes(byte[] uuid)
    {
    	return timeUuidFromBytes(uuid).toString();
    }

    /**
     * Converts the backing array to the appropriate object instance handling nulls.
     * <p/>
     * <p>Note: we could potentially depend on the {@link org.apache.cassandra.utils.ByteBufferUtil#string(java.nio.ByteBuffer, java.nio.charset.Charset)}.
     * Is it a public API?
     *
     * @return the deserialized instance or null if the backing array was null
     */
    public String toUTF8() {
        if (isNull()) return null;

        try {
            bytes.reset();
            return new String(this.bytes.array(), this.bytes.position(), this.bytes.remaining(), UTF8);
        } finally {
            this.bytes.reset();
        }
    }

    /**
     * Convert the byte buffer to a UTF-8 string.  The byte buffer will maintain it's original
     * {@link java.nio.ByteBuffer#position()}.
     *
     * @param bytes The UTF-8 string, encoded as raw bytes
     * @return The UTF-8 string object represented by the byte array
     */
    public static String toUTF8(ByteBuffer bytes) {
        if (bytes == null)
            return null;
        int position = bytes.position();
        try {
            return new String(bytes.array(), position, bytes.remaining(), UTF8);
        } finally {
            bytes.position(position);
        }
    }

    /**
     * Convert a raw byte array to a UTF-8 string
     *
     * @param bytes The UTF-8 string, encoded as raw bytes
     * @return The UTF-8 string object represented by the byte array
     */
    public static String toUTF8(byte[] bytes) {
        if (bytes == null)
            return null;
        return new String(bytes, UTF8);
    }

    /**
     * Transforms the provided list of {@link Bytes} instances into a list of byte arrays.
     *
     * @param arrays the list of Bytes instances
     * @return the list of byte arrays
     */
    public static Set<ByteBuffer> transformBytesToSet(Collection<Bytes> arrays) {
        if (arrays == null) return null;

        Set<ByteBuffer> transformed = new HashSet<ByteBuffer>(arrays.size());
        for (Bytes array : arrays) {
            transformed.add(array.getBytes());
        }

        return transformed;
    }

    /**
     * Transforms the provided list of {@link Bytes} instances into a list of byte arrays.
     *
     * @param arrays the list of Bytes instances
     * @return the list of byte arrays
     */
    public static List<ByteBuffer> transformBytesToList(Collection<Bytes> arrays) {
        if (arrays == null) return null;

        List<ByteBuffer> transformed = new ArrayList<ByteBuffer>(arrays.size());
        for (Bytes array : arrays) {
            transformed.add(array.getBytes());
        }

        return transformed;
    }

    /**
     * Transforms the provided list of {@link String} instances into a list of byte arrays.
     *
     * @param strings the list of String instances
     * @return the list of byte arrays
     */
    public static Set<ByteBuffer> transformUTF8ToSet(Collection<String> strings) {
        if (strings == null) return null;

        Set<ByteBuffer> transformed = new HashSet<ByteBuffer>(strings.size());
        for (String string : strings) {
            transformed.add(Bytes.fromUTF8(string).getBytes());
        }

        return transformed;
    }

    /**
     * Transforms the provided list of {@link String} instances into a list of byte arrays.
     *
     * @param strings the list of String instances
     * @return the list of byte arrays
     */
    public static List<ByteBuffer> transformUTF8ToList(Collection<String> strings) {
        if (strings == null) return null;

        List<ByteBuffer> transformed = new ArrayList<ByteBuffer>(strings.size());
        for (String string : strings) {
            transformed.add(Bytes.fromUTF8(string).getBytes());
        }

        return transformed;
    }

    /**
     * Returns the underlying byte array of the provided bytes instance or null if the provided instance was null.
     *
     * @param bytes the bytes instance
     * @return the underlying byte array of the instance or null
     */
    public static ByteBuffer nullSafeGet(Bytes bytes) {
        return bytes == null ? null : bytes.getBytes();
    }

    /**
     * Helper used to determine if the underlying {@link ByteBuffer} is null.
     *
     * @return true if null, otherwise false
     */
    public boolean isNull() {
        return this.bytes == null;
    }

    /**
     * Creates an instance of {@link CompositeBuilder} and assumes that the composite will be made up of two parts.
     * @return the composite composite
     */
    public static CompositeBuilder composite() {
        return new CompositeBuilder(2);
    }

    /**
     * Creates an instance of {@link CompositeBuilder} with a specified number of parts.
     * @param parts the number of parts the composite will be created from
     * @return the composite
     */
    public static CompositeBuilder composite(int parts) {
        return new CompositeBuilder(parts);
    }

    /**
     * A composite that's used to create instance of composite keys.
     */
    public static class CompositeBuilder {
        private List<Bytes> parts;

        public CompositeBuilder(int count) {
            parts = new ArrayList<Bytes>(count);
        }

        public CompositeBuilder addBoolean(boolean value) {
            parts.add(Bytes.fromBoolean(value));
            return this;
        }

        public CompositeBuilder addBoolean(Boolean value) {
            parts.add(Bytes.fromBoolean(value));
            return this;
        }

        public CompositeBuilder addByte(byte value) {
            parts.add(Bytes.fromByte(value));
            return this;
        }

        public CompositeBuilder addByte(Byte value) {
            parts.add(Bytes.fromByte(value));
            return this;
        }

        public CompositeBuilder addByteArray(byte[] value) {
            parts.add(Bytes.fromByteArray(value));
            return this;
        }

        public CompositeBuilder addByteBuffer(ByteBuffer value) {
            parts.add(Bytes.fromByteBuffer(value));
            return this;
        }

        public CompositeBuilder addChar(char value) {
            parts.add(Bytes.fromChar(value));
            return this;
        }

        public CompositeBuilder addChar(Character value) {
            parts.add(Bytes.fromChar(value));
            return this;
        }

        public CompositeBuilder addDouble(double value) {
            parts.add(Bytes.fromDouble(value));
            return this;
        }

        public CompositeBuilder addDouble(Double value) {
            parts.add(Bytes.fromDouble(value));
            return this;
        }

        public CompositeBuilder addFloat(float value) {
            parts.add(Bytes.fromFloat(value));
            return this;
        }

        public CompositeBuilder addFloat(Float value) {
            parts.add(Bytes.fromFloat(value));
            return this;
        }

        public CompositeBuilder addInt(int value) {
            parts.add(Bytes.fromInt(value));
            return this;
        }

        public CompositeBuilder addInt(Integer value) {
            parts.add(Bytes.fromInt(value));
            return this;
        }

        public CompositeBuilder addLong(long value) {
            parts.add(Bytes.fromLong(value));
            return this;
        }

        public CompositeBuilder addLong(Long value) {
            parts.add(Bytes.fromLong(value));
            return this;
        }

        public CompositeBuilder addShort(short value) {
            parts.add(Bytes.fromShort(value));
            return this;
        }

        public CompositeBuilder addShort(Short value) {
            parts.add(Bytes.fromShort(value));
            return this;
        }

        public CompositeBuilder addTimeUuid(UUID value) {
            parts.add(Bytes.fromTimeUuid(value));
            return this;
        }

        public CompositeBuilder addTimeUuid(com.eaio.uuid.UUID value) {
            parts.add(Bytes.fromTimeUuid(value));
            return this;
        }

        public CompositeBuilder addTimeUuid(String value) {
            parts.add(Bytes.fromTimeUuid(value));
            return this;
        }

        public CompositeBuilder addTimeUuid(long time, long clockSeqAndNode) {
            parts.add(Bytes.fromTimeUuid(time, clockSeqAndNode));
            return this;
        }

        public CompositeBuilder addUTF8(String str) {
            parts.add(Bytes.fromUTF8(str));
            return this;
        }

        public CompositeBuilder addUuid(UUID value) {
            parts.add(Bytes.fromUuid(value));
            return this;
        }

        public CompositeBuilder addUuid(String value) {
            parts.add(Bytes.fromUuid(value));
            return this;
        }

        public CompositeBuilder addUuid(long msb, long lsb) {
            parts.add(Bytes.fromUuid(msb, lsb));
            return this;
        }

        public Bytes build() {
            // we could avoid the duplicate loops by incrementing the length as we add parts...
            int length = 0;
            for (Bytes part : parts) {
                length += part.length();
            }

            final ByteBuffer buffer = ByteBuffer.allocate(length);
            for (Bytes part : parts) {
                buffer.put(part.getBytes());
            }

            return fromByteBuffer((ByteBuffer) buffer.rewind());
        }
    }
}