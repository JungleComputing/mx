package mxio;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class MxIOBuffer {
	
	protected ByteBuffer buf;
	
	private MxIOBuffer(ByteBuffer buf) {
		this.buf = buf;
	}
	
	public MxIOBuffer(int capacity) {
		buf = ByteBuffer.allocateDirect(capacity);
	}

	public MxIOBuffer slice() {
		return new MxIOBuffer(buf.slice());
	}

	public MxIOBuffer mark() {
		buf.mark();
		return this;
	}

	public MxIOBuffer reset() {
		buf.reset();
		return this;
	}

	public MxIOBuffer order(ByteOrder bo) {
		buf.order(bo);
		return this;
	}
	
	public ByteOrder order() {
		return buf.order();
	}
	
	public MxIOBuffer compact() {
		buf.compact();
		return this;
	}

	public MxIOBuffer duplicate() {
		return new MxIOBuffer(buf.duplicate());
	}

	public MxIOBuffer clear() {
		buf.clear();
		return this;
	}
	
	public MxIOBuffer flip() {
		buf.flip();
		return this;
	}
	
	public MxIOBuffer position(int newPosition) {
		buf.position(newPosition);
		return this;
	}
	
	public int position() {
		return buf.position();
	}
	
	public MxIOBuffer limit(int newLimit) {
		buf.limit(newLimit);
		return this;
	}
	
	public int limit() {
		return buf.limit();
	}
	
	public int capacity() {
		return buf.capacity();
	}
	
	public int remaining() {
		return buf.remaining();
	}
	
	public boolean hasRemaining() {
		return buf.hasRemaining();
	}
	
	public byte get() {
		return buf.get();
	}

	public byte get(int index) {
		return buf.get(index);
	}

	public MxIOBuffer get(byte[] dst, int off, int len) {
		buf.get(dst, off, len);
		return this;
	}
	
	public MxIOBuffer get(byte[] dst) {
		return get(dst, 0, dst.length);
	}
	
	public char getChar() {
		return buf.getChar();
	}

	public char getChar(int index) {
		return buf.getChar(index);
	}

	public double getDouble() {
		return buf.getDouble();
	}

	public double getDouble(int index) {
		return buf.getDouble();
	}

	public float getFloat() {
		return buf.getFloat();
	}

	public float getFloat(int index) {
		return buf.getFloat();
	}

	public int getInt() {
		return buf.getInt();
	}


	public int getInt(int index) {
		return buf.getInt(index);
	}

	public long getLong() {
		return buf.getLong();
	}

	public long getLong(int index) {
		return buf.getLong(index);
	}

	public short getShort() {
		return buf.getShort();
	}

	public short getShort(int index) {
		return buf.getShort(index);
	}

	public MxIOBuffer put(byte b) {
		buf.put(b);
		return this;
	}

	public MxIOBuffer put(int index, byte b) {
		buf.put(index, b);
		return this;
	}
	
	public MxIOBuffer put(byte[] src, int offset, int length) {
		buf.put(src, offset, length);
		return this;
	}
	
	public MxIOBuffer put(byte[] src) {
		buf.put(src, 0, src.length);
		return this;
	}

	public MxIOBuffer putChar(char value) {
		buf.putChar(value);
		return this;
	}

	public MxIOBuffer putChar(int index, char value) {
		buf.putChar(index, value);
		return this;
	}

	public MxIOBuffer putDouble(double value) {
		buf.putDouble(value);
		return this;
	}

	public MxIOBuffer putDouble(int index, double value) {
		buf.putDouble(index, value);
		return this;
	}

	public MxIOBuffer putFloat(float value) {
		buf.putFloat(value);
		return this;
	}

	public MxIOBuffer putFloat(int index, float value) {
		buf.putFloat(index, value);
		return this;
	}

	public MxIOBuffer putInt(int value) {
		buf.putInt(value);
		return this;
	}

	public MxIOBuffer putInt(int index, int value) {
		buf.putInt(index, value);
		return this;
	}

	public MxIOBuffer putLong(long value) {
		buf.putLong(value);
		return this;
	}

	public MxIOBuffer putLong(int index, long value) {
		buf.putLong(index, value);
		return this;
	}

	public MxIOBuffer putShort(short value) {
		buf.putShort(value);
		return this;
	}

	public MxIOBuffer putShort(int index, short value) {
		buf.putShort(index, value);
		return this;
	}
	
	public MxIOBuffer get(char[] dst, int off, int len) {
		int size = len * SizeOf.CHAR;
		int offset = off * SizeOf.CHAR;
		if(size > buf.remaining()) {
			throw new BufferUnderflowException();
		}
		int position = buf.position();
		doGet(dst, offset, buf, position, size);
		buf.position(position + size);
		return this;
	}
		
	public MxIOBuffer get(short[] dst, int off, int len) {
		int size = len * SizeOf.SHORT;
		int offset = off * SizeOf.SHORT;
		if(size > buf.remaining()) {
			throw new BufferUnderflowException();
		}
		int position = buf.position();
		doGet(dst, offset, buf, position, size);
		buf.position(position + size);
		return this;
	}
	
	public MxIOBuffer get(int[] dst, int off, int len) {
		int size = len * SizeOf.INT;
		int offset = off * SizeOf.INT;
		if(size > buf.remaining()) {
			throw new BufferUnderflowException();
		}
		int position = buf.position();
		doGet(dst, offset, buf, position, size);
		buf.position(position + size);
		return this;
	}
	
	public MxIOBuffer get(long[] dst, int off, int len) {
		int size = len * SizeOf.LONG;
		int offset = off * SizeOf.LONG;
		if(size > buf.remaining()) {
			throw new BufferUnderflowException();
		}
		int position = buf.position();
		doGet(dst, offset, buf, position, size);
		buf.position(position + size);
		return this;
	}
		
	public MxIOBuffer get(float[] dst, int off, int len) {
		int size = len * SizeOf.FLOAT;
		int offset = off * SizeOf.FLOAT;
		if(size > buf.remaining()) {
			throw new BufferUnderflowException();
		}
		int position = buf.position();
		doGet(dst, offset, buf, position, size);
		buf.position(position + size);
		return this;
	}
	
	public MxIOBuffer get(double[] dst, int off, int len) {
		int size = len * SizeOf.DOUBLE;
		int offset = off * SizeOf.DOUBLE;
		if(size > buf.remaining()) {
			throw new BufferUnderflowException();
		}
		int position = buf.position();
		doGet(dst, offset, buf, position, size);
		buf.position(position + size);
		return this;
	}
	
	public MxIOBuffer put(char[] src, int off, int len) {
		if(src.length < off + len) {
			throw new ArrayIndexOutOfBoundsException("" + src.length + " < " +  off+len);
		}
		int size = len * SizeOf.CHAR;
		int offset = off * SizeOf.CHAR;
		if(size > buf.remaining()) {
			throw new BufferOverflowException();
		}
		int position = buf.position();
		doPut(src, offset, buf, position, size);
		buf.position(position + size);
		return this;
	}
		
	public MxIOBuffer put(short[] src, int off, int len) {
		if(src.length < off + len) {
			throw new ArrayIndexOutOfBoundsException("" + src.length + " < " +  off+len);
		}
		int size = len * SizeOf.SHORT;
		int offset = off * SizeOf.SHORT;
		if(size > buf.remaining()) {
			throw new BufferOverflowException();
		}
		int position = buf.position();
		doPut(src, offset, buf, position, size);
		buf.position(position + size);
		return this;
	}
	
	public MxIOBuffer put(int[] src, int off, int len) {
		if(src.length < off + len) {
			throw new ArrayIndexOutOfBoundsException("" + src.length + " < " +  off+len);
		}
		int size = len * SizeOf.INT;
		int offset = off * SizeOf.INT;
		if(size > buf.remaining()) {
			throw new BufferOverflowException();
		}
		int position = buf.position();
		doPut(src, offset, buf, position, size);
		buf.position(position + size);
		return this;
	}
	
	public MxIOBuffer put(long[] src, int off, int len) {
		if(src.length < off + len) {
			throw new ArrayIndexOutOfBoundsException("" + src.length + " < " +  off+len);
		}
		int size = len * SizeOf.LONG;
		int offset = off * SizeOf.LONG;
		if(size > buf.remaining()) {
			throw new BufferOverflowException();
		}
		int position = buf.position();
		doPut(src, offset, buf, position, size);
		buf.position(position + size);
		return this;
	}
		
	public MxIOBuffer put(float[] src, int off, int len) {
		if(src.length < off + len) {
			throw new ArrayIndexOutOfBoundsException("" + src.length + " < " +  off+len);
		}
		int size = len * SizeOf.FLOAT;
		int offset = off * SizeOf.FLOAT;
		if(size > buf.remaining()) {
			throw new BufferOverflowException();
		}
		int position = buf.position();
		doPut(src, offset, buf, position, size);
		buf.position(position + size);
		return this;
	}
	
	public MxIOBuffer put(double[] src, int off, int len) {
		if(src.length < off + len) {
			throw new ArrayIndexOutOfBoundsException("" + src.length + " < " +  off+len);
		}
		int size = len * SizeOf.DOUBLE;
		int offset = off * SizeOf.DOUBLE;
		if(size > buf.remaining()) {
			throw new BufferOverflowException();
		}
		int position = buf.position();
		doPut(src, offset, buf, position, size);
		buf.position(position + size);
		return this;
	}
	
	private native void doGet(char[] dst, int off, 
			ByteBuffer src, int position, int size);
	
	private native void doGet(short[] dst, int off, 
			ByteBuffer src, int position, int size);
	
	private native void doGet(int[] dst, int off, 
			ByteBuffer src, int position, int size);
	
	private native void doGet(long[] dst, int off, 
			ByteBuffer src, int position, int size);
	
	private native void doGet(double[] dst, int off, 
			ByteBuffer src, int position, int size);
	
	private native void doGet(float[] dst, int off, 
			ByteBuffer src, int position, int size);

	private native void doPut(char[] src, int off, 
			ByteBuffer dst, int position, int size);
	
	private native void doPut(short[] src, int off, 
			ByteBuffer dst, int position, int size);
	
	private native void doPut(int[] src, int off, 
			ByteBuffer dst, int position, int size);
	
	private native void doPut(long[] src, int off, 
			ByteBuffer dst, int position, int size);
	
	private native void doPut(double[] src, int off, 
			ByteBuffer dst, int position, int size);
	
	private native void doPut(float[] src, int off, 
			ByteBuffer dst, int position, int size);

}