package org.scale7.cassandra.pelops;

public class UuidHelper {
	/**
	 * Generate a new time UUID object
	 * @return							A new time UUID object
	 */
	public static java.util.UUID newTimeUuid()
	{
		return java.util.UUID.fromString(new com.eaio.uuid.UUID().toString());
	}
	
	/**
	 * Generate a new time UUID in serialized form
	 * @return							The serialized bytes of a new time UUID object
     * @deprecated Use the methods available on {@link Bytes}
	 */
    @Deprecated
	public static byte[] newTimeUuidBytes()
	{
		return timeUuidToBytes(newTimeUuid());
	}

	/**
	 * Deserializes a TimeUUID from a byte array
	 * @param uuid						The bytes of the time UUID
	 * @return							The deserialized time UUID object
     * @deprecated Use the methods available on {@link Bytes}
	 */
    @Deprecated
	public static java.util.UUID timeUuidFromBytes( byte[] uuid )
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
	 * Serialize a time UUID into a byte array.
	 * @param uuid						The time UUID to serialize
	 * @return							The serialized bytes of the time UUID
     * @deprecated Use the methods available on {@link Bytes}
	 */
    @Deprecated
	public static byte[] timeUuidToBytes(java.util.UUID uuid)
	{
		long msb = uuid.getMostSignificantBits();
		long lsb = uuid.getLeastSignificantBits();
		
		return uuidToBytes(msb, lsb);
	}

    /**
     * @deprecated Use the methods available on {@link Bytes}
     */
    @Deprecated
	public static byte[] uuidToBytes(long msb, long lsb) {
				
		byte[] buffer = new byte[16];

		for (int i = 0; i < 8; i++) {
			buffer[i] = (byte) (msb >>> 8 * (7 - i));
		}
		for (int i = 8; i < 16; i++) {
			buffer[i] = (byte) (lsb >>> 8 * (7 - i));
		}

		return buffer;
	}
	
	/**
	 * Convert a string representation of a time UUID into bytes
	 * @param uuidStr					The string representation of the time UUID
	 * @return							The serialized bytes of the represented time UUID object
     * @deprecated Use the methods available on {@link Bytes}
	 */
    @Deprecated
	public static byte[] timeUuidStringToBytes(String uuidStr) {
		return timeUuidToBytes(java.util.UUID.fromString(uuidStr));
	}
}
