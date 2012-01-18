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

package org.scale7.cassandra.pelops;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

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
    public static final Bytes EMPTY = fromByteArray(new byte[0]);
    public static final Bytes NULL = fromByteBuffer(null);

    private final ByteBuffer bytes;
    private int hashCode = -1;

    /**
     * Constructs a new instance based on the provided byte array.
     *
     * @param bytes the bytes
     * @deprecated use {@link #fromByteArray(byte[])} instead
     */
    @Deprecated
    public Bytes(byte[] bytes) {
        this(ByteBuffer.wrap(bytes), false);
    }

    /**
     * Constructs a new instance based on the provided byte buffer.  The {@link java.nio.ByteBuffer#position()} must be
     * in the correct position to read the appropriate value from it.
     *
     * @param bytes the bytes
     * @deprecated use {@link #fromByteBuffer(java.nio.ByteBuffer)} instead
     */
    @Deprecated
    public Bytes(ByteBuffer bytes) {
        this(bytes, true);
    }

    /**
     * Constructs a new instance based on the provided byte buffer.  The {@link java.nio.ByteBuffer#position()} must be
     * in the correct position to read the appropriate value from it. The given bytes are either used directly,
     * or duplicated.
     *
     * @param bytes the bytes
     * @param duplicate if true, duplicates the given bytes; otherwise, it is rewound and used it as-is
     */
    private Bytes(ByteBuffer bytes, boolean duplicate) {
        if (bytes != null) {
            this.bytes = duplicate ? bytes.duplicate() : (ByteBuffer)bytes.rewind();
            this.bytes.mark();
        } else
            this.bytes = null;
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
        return isNull() ? Bytes.NULL : new Bytes(bytes, true);
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
        return new Bytes(BufferHelper.fromByteArray(value), false);
    }

    /**
     * Creates an instance based on the provided byte buffer.
     *
     * @param value the value
     * @return the Bytes instance
     */
    public static Bytes fromByteBuffer(ByteBuffer value) {
        return new Bytes(value, true);
    }

    /**
     * Creates an instance based on the provided value.
     *
     * @param value the value
     * @return the instance
     * @see java.nio.ByteBuffer for details on serializaion format
     */
    public static Bytes fromChar(char value) {
        return new Bytes(BufferHelper.fromChar(value), false);
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
        return new Bytes(BufferHelper.fromByte(value), false);
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
        return new Bytes(BufferHelper.fromLong(value), false);
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
        return new Bytes(BufferHelper.fromInt(value), false);
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
        return new Bytes(BufferHelper.fromShort(value), false);
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
        return new Bytes(BufferHelper.fromDouble(value), false);
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
        return new Bytes(BufferHelper.fromFloat(value), false);
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
        return new Bytes(BufferHelper.fromBoolean(value), false);
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
        return value == null ? NULL : new Bytes(BufferHelper.fromUuid(value), false);
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
        return value == null ? NULL : new Bytes(BufferHelper.fromUuid(value), false);
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
        return new Bytes(BufferHelper.fromUuid(msb, lsb), false);
    }

    /**
     * Creates an instance based on the provided value handling nulls.  If the provided value is not null then this method
     * delegates to {@link #fromUuid(long, long)} to perform the deserialization.
     *
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on long serializaion format
     */
    public static Bytes fromTimeUuid(com.eaio.uuid.UUID value) {
        return value == null ? NULL : fromUuid(value.getTime(), value.getClockSeqAndNode());
    }

    /**
     * Creates an instance based on the provided value handling nulls.
     *
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on long serializaion format
     * @deprecated use {@link #fromUuid(UUID)} instead
     */
    @Deprecated
    public static Bytes fromTimeUuid(UUID value) {
        return fromUuid(value);
    }

    /**
     * Creates an instance based on the provided value handling nulls.
     *
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on long serializaion format
     * @deprecated use {@link #fromUuid(String)} instead
     */
    @Deprecated
    public static Bytes fromTimeUuid(String value) {
        return fromUuid(value);
    }

    /**
     * Creates an instance based on the provided values.
     *
     * @param time            the time value
     * @param clockSeqAndNode the clockSeqAndNode value
     * @return the instance or null if the value provided was null
     * @see java.nio.ByteBuffer for details on long serializaion format
     * @deprecated use {@link #fromUuid(long, long)} instead
     */
    @Deprecated
    public static Bytes fromTimeUuid(long time, long clockSeqAndNode) {
        return fromUuid(time, clockSeqAndNode);
    }

    /**
     * Creates an instance based on the provided value in UTF-8 format handling nulls.
     *
     * @param value the value
     * @return the instance or null if the value provided was null
     * @see String#getBytes(java.nio.charset.Charset) for details on the format
     */
    public static Bytes fromUTF8(String value) {
        return value == null ? NULL : new Bytes(BufferHelper.fromUTF8(value), false);
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
     * Pads a byte buffer to match a specified length.
     *
     * @param totalLength the length to pad the buffer
     * @param originalByteBuffer the buffer with data to pad
     * @return the padded buffer; this may be the original buffer or a new buffer
     */
    private static ByteBuffer padValue(int totalLength, ByteBuffer originalByteBuffer) {
        if (originalByteBuffer.capacity() >= totalLength)
            return originalByteBuffer;

        byte[] originalByteArray = originalByteBuffer.array();
        byte[] newByteArray = new byte[totalLength];
        for (int i = originalByteArray.length - 1; i >= 0; i--)
            newByteArray[(totalLength - 1) - i] = originalByteArray[i];

        return ByteBuffer.wrap(newByteArray);
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
            return padValue(8, this.bytes).getLong();
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
            return padValue(4, this.bytes).getInt();
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
            return padValue(2, this.bytes).getShort();
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
            return this.bytes.get() != BufferHelper.BOOLEAN_FALSE;
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
        UUID uuid = toUuid();
        if (uuid == null) return null;
        return new com.eaio.uuid.UUID(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    /**
     * Efficiently constructs a uuid from a bytes array without creating any intermediate objects
     *
     * @param uuid The bytes representing the uuid.
     * @return A uuid object
     */
    public static UUID uuidFromBytes(byte[] uuid) {
        long msb = 0;
        long lsb = 0;
        assert uuid.length == 16;
        for (int i = 0; i < 8; i++)
            msb = (msb << 8) | (uuid[i] & 0xff);
        for (int i = 8; i < 16; i++)
            lsb = (lsb << 8) | (uuid[i] & 0xff);
        return new UUID(msb, lsb);
    }

    /**
     * Efficiently constructs a uuid from a bytes array without creating any intermediate objects
     *
     * @param uuid The bytes representing the uuid.
     * @return A uuid object
     * @deprecated use {@link #uuidFromBytes(byte[])} instead
     */
    @Deprecated
    public static UUID timeUuidFromBytes(byte[] uuid) {
        return uuidFromBytes(uuid);
    }

    /**
     * Createa a UTF-8 representation of a uuid from a bytes array
     * @param uuid The bytes representing the uuid.
     * @return A string representation of the uuid
     */
    public static String utf8UuidFromBytes(byte[] uuid) {
    	return uuidFromBytes(uuid).toString();
    }

    /**
     * Createa a UTF-8 representation of a uuid from a bytes array
     * @param uuid The bytes representing the uuid.
     * @return A string representation of the uuid
     * @deprecated use {@link #utf8UuidFromBytes(byte[])} instead
     */
    @Deprecated
    public static String utf8TimeUuidFromBytes(byte[] uuid) {
    	return utf8UuidFromBytes(uuid);
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
            return new String(this.bytes.array(), this.bytes.position(), this.bytes.remaining(), BufferHelper.UTF8);
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
            return new String(bytes.array(), position, bytes.remaining(), BufferHelper.UTF8);
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
        return new String(bytes, BufferHelper.UTF8);
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
     * @deprecated Deprecated in {@link org.scale7.cassandra.pelops.types.CompositeType}. NOTE: This method generates composites that are not compatible with the official Cassandra {@link org.scale7.cassandra.pelops.types.CompositeType composite type}.
     */
    public static CompositeBuilder composite() {
        return new CompositeBuilder(2);
    }

    /**
     * Creates an instance of {@link CompositeBuilder} with a specified number of parts.
     * @param parts the number of parts the composite will be created from
     * @return the composite
     * @deprecated Deprecated in {@link org.scale7.cassandra.pelops.types.CompositeType}. NOTE: This method generates composites that are not compatible with the official Cassandra {@link org.scale7.cassandra.pelops.types.CompositeType composite type}.
     * */
    public static CompositeBuilder composite(int parts) {
        return new CompositeBuilder(parts);
    }

    /**
     * A composite that's used to create instance of composite keys.
     * @deprecated Deprecated in {@link org.scale7.cassandra.pelops.types.CompositeType}. NOTE: This class generates composites that are not compatible with the official Cassandra {@link org.scale7.cassandra.pelops.types.CompositeType composite type}.
     */
    public static class CompositeBuilder {
        private List<ByteBuffer> parts;
        private int length;

        public CompositeBuilder(int count) {
            parts = new ArrayList<ByteBuffer>(count);
        }

        public CompositeBuilder addByteBuffer(ByteBuffer value) {
            parts.add(value);
            length += value.rewind().remaining();
            return this;
        }

        public CompositeBuilder addBytes(Bytes value) {
            return addByteBuffer(value.getBytes());
        }

        public CompositeBuilder addBoolean(boolean value) {
            return addByteBuffer(BufferHelper.fromBoolean(value));
        }

        public CompositeBuilder addBoolean(Boolean value) {
            return addByteBuffer(BufferHelper.fromBoolean(value));
        }

        public CompositeBuilder addByte(byte value) {
            return addByteBuffer(BufferHelper.fromByte(value));
        }

        public CompositeBuilder addByte(Byte value) {
            return addByteBuffer(BufferHelper.fromByte(value));
        }

        public CompositeBuilder addByteArray(byte[] value) {
            return addByteBuffer(BufferHelper.fromByteArray(value));
        }

        public CompositeBuilder addChar(char value) {
            return addByteBuffer(BufferHelper.fromChar(value));
        }

        public CompositeBuilder addChar(Character value) {
            return addByteBuffer(BufferHelper.fromChar(value));
        }

        public CompositeBuilder addDouble(double value) {
            return addByteBuffer(BufferHelper.fromDouble(value));
        }

        public CompositeBuilder addDouble(Double value) {
            return addByteBuffer(BufferHelper.fromDouble(value));
        }

        public CompositeBuilder addFloat(float value) {
            return addByteBuffer(BufferHelper.fromFloat(value));
        }

        public CompositeBuilder addFloat(Float value) {
            return addByteBuffer(BufferHelper.fromFloat(value));
        }

        public CompositeBuilder addInt(int value) {
            return addByteBuffer(BufferHelper.fromInt(value));
        }

        public CompositeBuilder addInt(Integer value) {
            return addByteBuffer(BufferHelper.fromInt(value));
        }

        public CompositeBuilder addLong(long value) {
            return addByteBuffer(BufferHelper.fromLong(value));
        }

        public CompositeBuilder addLong(Long value) {
            return addByteBuffer(BufferHelper.fromLong(value));
        }

        public CompositeBuilder addShort(short value) {
            return addByteBuffer(BufferHelper.fromShort(value));
        }

        public CompositeBuilder addShort(Short value) {
            return addByteBuffer(BufferHelper.fromShort(value));
        }

        public CompositeBuilder addUTF8(String str) {
            return addByteBuffer(BufferHelper.fromUTF8(str));
        }

        public CompositeBuilder addUuid(UUID value) {
            return addByteBuffer(BufferHelper.fromUuid(value));
        }

        public CompositeBuilder addUuid(String value) {
            return addByteBuffer(BufferHelper.fromUuid(value));
        }

        public CompositeBuilder addUuid(long msb, long lsb) {
            return addByteBuffer(BufferHelper.fromUuid(msb, lsb));
        }

        public CompositeBuilder addTimeUuid(com.eaio.uuid.UUID value) {
            return addByteBuffer(BufferHelper.fromUuid(value.getTime(), value.getClockSeqAndNode()));
        }

        /** @deprecated use {@link #addUuid(UUID)} instead */
        @Deprecated
        public CompositeBuilder addTimeUuid(UUID value) {
            return addUuid(value);
        }

        /** @deprecated use {@link #addUuid(String)} instead */
        @Deprecated
        public CompositeBuilder addTimeUuid(String value) {
            return addUuid(value);
        }

        /** @deprecated use {@link #addUuid(long, long)} instead */
        @Deprecated
        public CompositeBuilder addTimeUuid(long time, long clockSeqAndNode) {
            return addUuid(time, clockSeqAndNode);
        }

        public Bytes build() {
            final ByteBuffer buffer = ByteBuffer.allocate(length);
            for (ByteBuffer part : parts) {
                buffer.put(part);
            }

            return new Bytes(buffer, false);
        }
    }

    /**
     * Encapsulates ByteBuffer allocation.
     */
    public static class BufferHelper {

        static final int SIZEOF_BYTE = Byte.SIZE / Byte.SIZE;
        static final int SIZEOF_BOOLEAN = SIZEOF_BYTE;
        static final byte BOOLEAN_TRUE = (byte)1;
        static final byte BOOLEAN_FALSE = (byte)0;
        static final int SIZEOF_CHAR = Character.SIZE / Byte.SIZE;
        static final int SIZEOF_SHORT = Short.SIZE / Byte.SIZE;
        static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;
        static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;
        static final int SIZEOF_FLOAT = Float.SIZE / Byte.SIZE;
        static final int SIZEOF_DOUBLE = Double.SIZE / Byte.SIZE;
        static final int SIZEOF_UUID = SIZEOF_LONG + SIZEOF_LONG;

        public static final Charset UTF8 = Charset.forName("UTF-8");
        
        // Utility class
        private BufferHelper(){}

        public static ByteBuffer fromByteArray(byte[] value) {
            return ByteBuffer.wrap(value);
        }

        public static ByteBuffer fromChar(char value) {
            return ByteBuffer.allocate(SIZEOF_CHAR).putChar(value);
        }

        public static ByteBuffer fromByte(byte value) {
            return ByteBuffer.allocate(SIZEOF_BYTE).put(value);
        }

        public static ByteBuffer fromLong(long value) {
            return ByteBuffer.allocate(SIZEOF_LONG).putLong(value);
        }

        public static ByteBuffer fromInt(int value) {
            return ByteBuffer.allocate(SIZEOF_INT).putInt(value);
        }

        public static ByteBuffer fromShort(short value) {
            return ByteBuffer.allocate(SIZEOF_SHORT).putShort(value);
        }

        public static ByteBuffer fromDouble(double value) {
            return ByteBuffer.allocate(SIZEOF_DOUBLE).putDouble(value);
        }

        public static ByteBuffer fromFloat(float value) {
            return ByteBuffer.allocate(SIZEOF_FLOAT).putFloat(value);
        }

        public static ByteBuffer fromBoolean(boolean value) {
            return ByteBuffer.allocate(SIZEOF_BOOLEAN).put(value ? BOOLEAN_TRUE : BOOLEAN_FALSE);
        }

        public static ByteBuffer fromUuid(UUID value) {
            return fromUuid(value.getMostSignificantBits(), value.getLeastSignificantBits());
        }

        public static ByteBuffer fromUuid(String value) {
            return fromUuid(UUID.fromString(value));
        }

        public static ByteBuffer fromUuid(long msb, long lsb) {
            return ByteBuffer.allocate(SIZEOF_UUID).putLong(msb).putLong(lsb);
        }

        public static ByteBuffer fromUTF8(String value) {
            return fromByteArray(value.getBytes(UTF8));
        }
    }
}