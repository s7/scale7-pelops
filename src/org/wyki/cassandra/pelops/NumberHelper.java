package org.wyki.cassandra.pelops;

public class NumberHelper {

	public static final long toLong(byte[] b) {
		return toLong(b,0,b.length);
	}
	
	public static final long toLong(byte[] b,int offset,int size) {
		long l = 0;
		for (int i=0; i<size; ++i)
			l |= ((long)b[offset+i]&0xff)<<((size-i-1)<<3);
		return l;
	}
	
	public static final int toInt(byte[] b,int offset) {
		return b[offset+0]<<24 | (b[offset+1]&0xff)<<16 | (b[offset+2]&0xff)<<8 | (b[offset+3]&0xff);
	}
	
	public static final int toInt(byte[] b) {
		return b[0]<<24 | (b[1]&0xff)<<16 | (b[2]&0xff)<<8 | (b[3]&0xff);
	}
	
	public static final short toShort(byte[] b,int offset) {
		return (short)((b[offset+0]&0xff)<<8 | (b[offset+1]&0xff));
	}
	
	public static final short toShort(byte[] b) {
		return (short)((b[0]&0xff)<<8 | (b[1]&0xff));
	}
 

	public static final byte[] toBytes(long l) {
		return toBytes(l, 8);
	}
	
	public static final byte[] toBytes(long l,int size) {
		byte[] b = new byte[size];
		toBytes(l,b,0,size);
		return b;
	}
	
	public static final void toBytes(long l, byte[] b, int offset, int size) {
		for (int i=0; i<size; ++i)
			b[offset+i] = (byte)(l>>((size-i-1)<<3));
	}
	
	public static final byte[] toBytes(int i) {
        return new byte[] { (byte)(i >>> 24), (byte)(i >>> 16), (byte)(i >>> 8), (byte)i};
	}
	
	public static final byte[] toBytes(short i) {
		return new byte[] { (byte)(i>>8), (byte)i };
	}
	
	public static final void toBytes(short i,byte[] b,int offset) {
		b[offset++] = (byte)(i>>8);
		b[offset++] = (byte)i;
	}
	
	public static final void toBytes(int i,byte[] b,int offset) {
		b[offset++] = (byte)(i>>24);
		b[offset++] = (byte)(i>>16);
		b[offset++] = (byte)(i>>8);
		b[offset++] = (byte)i;
	}
}
