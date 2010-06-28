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
			throw new IllegalStateException(e);
		}
	}
	
	/**
	 * Create a <code>String</code> from an array of UTF-8 bytes
	 * @param bytes							The array of UTF-8 bytes
	 * @return								The <code>String</code> object
	 */
	public static String toUTF8(byte[] bytes) {
		if (bytes == null)
			return null;
        try {
            return new String(bytes, "utf-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }
}
