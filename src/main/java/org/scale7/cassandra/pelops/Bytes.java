package org.scale7.cassandra.pelops;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * The byte array class.
 */
public class Bytes {
    public static final Bytes EMPTY = new Bytes(new byte[0]);
    
    private byte[] bytes;

    public Bytes(byte[] bytes) {
    	if (bytes == null)
    		bytes = new byte[]{};
        this.bytes = bytes;
    }

    /**
     * Returns a string representation of the bytes as defined by the {@link java.util.Arrays#toString(byte[])} method.
     * <p><b>NOTE</b>: The {@link #toUTF8()} method provides the reverse value of the {@link #fromUTF8(String)} method.
     * @return the string representation
     */
    @Override
    public String toString() {
        return Arrays.toString(this.bytes);
    }

    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Bytes)) return false;

        Bytes byteArray = (Bytes) o;

        if (!Arrays.equals(bytes, byteArray.bytes)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    public int length() {
        return this.bytes.length;
    }

    public static Bytes fromBytes(byte[] value) {
        return value != null ? new Bytes(value) : null;
    }

    public static Bytes fromLong(long value) {
        return new Bytes(NumberHelper.toBytes(value));
    }

    public static Bytes fromInt(int value) {
        return new Bytes(NumberHelper.toBytes(value));
    }

    public static Bytes fromUuid(UUID value) {
        return value != null ? new Bytes(UuidHelper.timeUuidToBytes(value)) : null;
    }

    public static Bytes fromUuid(String value) {
        return value != null ? new Bytes(UuidHelper.timeUuidStringToBytes(value)) : null;
    }

    public static Bytes fromUTF8(String value) {
        return value != null ? new Bytes(StringHelper.toBytes(value)) : null;
    }

    public long toLong() {
        return NumberHelper.toLong(this.bytes);
    }

    public int toInt() {
        return NumberHelper.toInt(this.bytes);
    }

    public UUID toUuid() {
        return UuidHelper.timeUuidFromBytes(this.bytes);
    }

    public String toUTF8() {
        return StringHelper.toUTF8(this.bytes);
    }

    public static List<byte[]> transform(List<Bytes> arrays) {
        if (arrays == null) return null;

        List<byte[]> transformed = new ArrayList<byte[]>(arrays.size());
        for (Bytes array : arrays) {
            transformed.add(array.getBytes());
        }

        return transformed;
    }

    public static byte[] nullSafeGet(Bytes bytes) {
        return bytes == null ? null : bytes.getBytes();
    }
}