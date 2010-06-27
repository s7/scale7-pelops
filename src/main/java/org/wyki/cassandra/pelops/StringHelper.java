package org.wyki.cassandra.pelops;

import java.io.UnsupportedEncodingException;

public class StringHelper {
	
	/**
	 * Create an array of bytes that represents a <code>String</code> using UTF-8 encoding.
	 * @param string						The <code>String</code> to convert
	 * @return								The string as UTF-8 bytes
	 */
	public static byte[] toBytes(String string) {
		try {
			return string.getBytes("utf-8");
		} catch (UnsupportedEncodingException e) {
			assert false;
			e.printStackTrace();
			return new byte[] {};
		}
	}
	
	/**
	 * Create a <code>String</code> from an array of UTF-8 bytes
	 * @param bytes							The array of UTF-8 bytes
	 * @return								The <code>String</code> object
	 * @throws UnsupportedEncodingException	Thrown if the bytes are not represent an UTF-8 encoding
	 */
	public static String toUTF8(byte[] bytes) throws UnsupportedEncodingException {
		if (bytes == null)
			return null;
		return new String(bytes, "utf-8");
	}
}
