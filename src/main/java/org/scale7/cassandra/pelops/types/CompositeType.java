package org.scale7.cassandra.pelops.types;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Bytes.BufferHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CompositeType utility class
 *
 * @author Ali Serghini
 */
public class CompositeType {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompositeType.class);

    //Utility class
    private CompositeType() {
    }

    /**
     * A composite that's used to create instance of composite keys.
     */
    public static class Builder {

        private List<ByteBuffer> parts = null;

        private Builder(int count) {
            parts = new ArrayList<ByteBuffer>(count);
        }

        /**
         * Creates a new builder
         *
         * @param partsCount - CompositeType parts count. Should be 2 or more.
         * @return CompositeType builder
         */
        public static Builder newBuilder(int partsCount) {
            if (partsCount < 1) throw new RuntimeException("Invalid parts count. Should be 2 or more.");
            return new Builder(partsCount);
        }

        /**
         * Creates a new builder. Assumes that there will be 2 elements in the composite type
         *
         * @param partsCount - CompositeType parts count
         * @return CompositeType builder
         */
        public static Builder newBuilder() {
            return new Builder(2);
        }

        public Builder addByteBuffer(ByteBuffer value) {
            parts.add(value);
            return this;
        }

        public Builder addBytes(Bytes value) {
            return addByteBuffer(value.getBytes());
        }

        public Builder addBoolean(boolean value) {
            return addByteBuffer(BufferHelper.fromBoolean(value));
        }

        public Builder addBoolean(Boolean value) {
            return addByteBuffer(BufferHelper.fromBoolean(value));
        }

        public Builder addByte(byte value) {
            return addByteBuffer(BufferHelper.fromByte(value));
        }

        public Builder addByte(Byte value) {
            return addByteBuffer(BufferHelper.fromByte(value));
        }

        public Builder addByteArray(byte[] value) {
            return addByteBuffer(BufferHelper.fromByteArray(value));
        }

        public Builder addChar(char value) {
            return addByteBuffer(BufferHelper.fromChar(value));
        }

        public Builder addChar(Character value) {
            return addByteBuffer(BufferHelper.fromChar(value));
        }

        public Builder addDouble(double value) {
            return addByteBuffer(BufferHelper.fromDouble(value));
        }

        public Builder addDouble(Double value) {
            return addByteBuffer(BufferHelper.fromDouble(value));
        }

        public Builder addFloat(float value) {
            return addByteBuffer(BufferHelper.fromFloat(value));
        }

        public Builder addFloat(Float value) {
            return addByteBuffer(BufferHelper.fromFloat(value));
        }

        public Builder addInt(int value) {
            return addByteBuffer(BufferHelper.fromInt(value));
        }

        public Builder addInt(Integer value) {
            return addByteBuffer(BufferHelper.fromInt(value));
        }

        public Builder addLong(long value) {
            return addByteBuffer(BufferHelper.fromLong(value));
        }

        public Builder addLong(Long value) {
            return addByteBuffer(BufferHelper.fromLong(value));
        }

        public Builder addShort(short value) {
            return addByteBuffer(BufferHelper.fromShort(value));
        }

        public Builder addShort(Short value) {
            return addByteBuffer(BufferHelper.fromShort(value));
        }

        public Builder addUTF8(String str) {
            return addByteBuffer(BufferHelper.fromUTF8(str));
        }

        public Builder addUuid(UUID value) {
            return addByteBuffer(BufferHelper.fromUuid(value));
        }

        public Builder addUuid(String value) {
            return addByteBuffer(BufferHelper.fromUuid(value));
        }

        public Builder addUuid(long msb, long lsb) {
            return addByteBuffer(BufferHelper.fromUuid(msb, lsb));
        }

        public Builder addTimeUuid(com.eaio.uuid.UUID value) {
            return addByteBuffer(BufferHelper.fromUuid(value.getTime(), value.getClockSeqAndNode()));
        }

        /**
         * Reset the builder
         */
        public void clear() {
            parts.clear();
        }

        public Bytes build() throws Exception {
            if (parts == null || parts.isEmpty()) return null;

            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            for (ByteBuffer part : parts) {
                if (!part.hasArray()) throw new Exception("Invalid composite byte part encountered");

                bos.write((byte) ((part.array().length >> (7 + 1)) & 0xFF));
                bos.write((byte) (part.array().length & 0xFF));
                for (byte partByte : part.array()) {
                    bos.write(partByte & 0xFF);
                }

                bos.write((byte) 0);
            }

            final Bytes bytes = Bytes.fromByteArray(bos.toByteArray());

            try {
                bos.close();
            } catch (IOException ex) {
                LOGGER.error("Failed to close the compostite type output stream", ex);
            }

            return bytes;
        }
    }

    /**
     * Parses the composite key
     *
     * @param compositeKey - composite key as Bytes
     * @return list of the composite key elements
     * @throws Exception
     */
    public static List<byte[]> parseCompositeType(Bytes compositeKey) throws Exception {
        if (compositeKey == null) return null;
        return parseCompositeType(compositeKey.toByteArray());
    }

    /**
     * Parses the composite key
     *
     * @param compositeKey - composite key as byte array
     * @return list of the composite key elements
     * @throws Exception
     */
    public static List<byte[]> parseCompositeType(byte[] compositeKey) throws Exception {
        if (compositeKey == null) return null;

        int ndx = 0;
        //Validate the array length
        if (compositeKey.length < (ndx + 2)) throw new Exception("Invalid Composite type structure");

        final List<byte[]> list = new ArrayList<byte[]>(3);//Default to 3
        while (compositeKey.length > (ndx + 2)) {
            // Value length is a 2 bytes short
            short length = ByteBuffer.wrap(Arrays.copyOfRange(compositeKey, ndx, (ndx + 2))).getShort();
            int componentEndNdx = (ndx + 2 + length);

            // Check if the component legth is valid
            if (compositeKey.length < componentEndNdx + 1) throw new Exception("Invalid Composite type structure");
            // If the value is not properly terminated throw an exception
            if (compositeKey[componentEndNdx] != (byte) 0)
                throw new Exception("Invalid Composite type structure: Not properly terminated, should be 0 byte terminated, found " + compositeKey[componentEndNdx]);

            // Get the value
            list.add(Arrays.copyOfRange(compositeKey, (ndx + 2), componentEndNdx));

            // Update the value of the index
            ndx = componentEndNdx + 1;
        }

        return list;
    }
}
