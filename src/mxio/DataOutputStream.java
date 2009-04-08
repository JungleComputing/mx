package mxio;

import java.io.IOException;
import java.nio.BufferOverflowException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.touchgraph.graphlayout.TGPanel.SwitchSelectUI;

public abstract class DataOutputStream extends ibis.io.DataOutputStream {
	
	private static final Logger logger = LoggerFactory
    .getLogger(DataOutputStream.class);

	private MxSendBuffer buffer;

	long bytesWritten = 0;
	boolean flushed = true;

	boolean closed = false;
	boolean receiverClosed = false;
	int sequenceNo = 1;
	
	

	MulticastDataOutputStream mcStream = null;

	protected DataOutputStream() {
		nextBuffer();
	}

	public boolean isOpen() {
		return !closed;
	}

	void nextBuffer() {
		buffer = MxSendBuffer.get();
		if(sequenceNo <= Config.SEQ_STEPS) {
			buffer.setLimit(sequenceNo * Config.SEQ_SIZE * Config.SEQ_MULTIPLIER);
			sequenceNo++;
		}
	}
	
	// closes all underlying connections
	@Override
	public void close() throws IOException {
		if (logger.isDebugEnabled()) {
			logger.debug("close()");
		}
		if (closed) {
			return;
		}
		closed = true;
		if(mcStream == null) {
			try {
				if (!buffer.isEmpty()) {
					buffer.flip();
					bytesWritten += doSend(buffer);
				} else {
					MxSendBuffer.recycle(buffer);
				}
				buffer = null;
			} catch (IOException e1) {
				// ignore
			}
		}
		try {
			doFlush();
			doClose();
		} catch (IOException e) {
			// ignore
		}
	}

	final void addToMulticast(MulticastDataOutputStream stream) throws IOException {
		if(mcStream != null) {
			// can only be part of 1 mcStream
			throw new IOException("Already in MulticastOutputStream");
		}
		
		flush(); // throws an IOException

		MxSendBuffer.recycle(buffer);
		buffer = null;
		mcStream = stream;
	}

	final void removeFromMulticast() {
		if(mcStream == null) {
			//huh, cannot happen. At the other hand, still nothing is wrong with this channel 
			return;
		}
		nextBuffer();
	}

	protected void send() throws IOException {
		if (buffer.isEmpty()) {
			return;
		}
		buffer.flip();
		bytesWritten += doSend(buffer);

		// get a new buffer
		nextBuffer();
		flushed = false;
	}

	@Override
	public void flush() throws IOException {
		if (closed) {
			throw new IOException("Stream is closed");
		}
		if (receiverClosed) {
			doClose();
			throw new IOException("Stream is closed by receiver");
		}
		
		if(buffer != null) {
			send();
		}
		doFlush();
		flushed = true;
		sequenceNo = 1;
	}

	public int bufferSize() {
		return Config.REPORTED_BUFFER_SIZE;
	}

	abstract long doSend(MxSendBuffer buffer) throws IOException;

	abstract void doFlush() throws IOException;

	abstract void doClose() throws IOException;

	public long bytesWritten() {
		return bytesWritten;
	}

	public void resetBytesWritten() {
		bytesWritten = 0;
	}

	public void writeByte(byte value) throws IOException {
		if(closed) {
			throw new IOException("Stream is closed");
		}
		if(receiverClosed) {
			doClose();
			throw new IOException("Stream is closed by receiver");
		}
		while(buffer.remaining() < SizeOf.BYTE) {
			send();
			if(closed) {
				throw new IOException("Stream is closed");
			}
			if(receiverClosed) {
				doClose();
				throw new IOException("Stream is closed by receiver");
			}
		}
		buffer.payload.put(value);
		startupSend();
	}

	public void writeArray(byte[] array, int off, int len) throws IOException {
		if(closed) {
			throw new IOException("Stream is closed");
		}
		if(receiverClosed) {
			doClose();
			throw new IOException("Stream is closed by receiver");
		}
			 
		int remaining = len;
		int offset = off;
		while (remaining > 0) {
			if (buffer.payload.remaining() < SizeOf.BYTE) {
				send();
			}

			int size = Math.min(remaining, buffer.payload.remaining() / SizeOf.BYTE);
			buffer.payload.put(array, offset, size);
			offset += size;
			remaining -= size;
		}
		startupSend();
	}

	public void write(int value) throws IOException {
		writeByte((byte) value);
	}
	
	public void write(byte[] b, int off, int len) throws IOException {
		writeArray(b, off, len);
	}


	@Override
	public void writeArray(boolean[] array, int off, int len)
			throws IOException {
		int last = off + len;
		for (int i = off; i < last; i++) {
			writeBoolean(array[i]);
		}
	}

	@Override
	public void writeArray(char[] array, int off, int len) throws IOException {
		if(closed) {
			throw new IOException("Stream is closed");
		}
		if(receiverClosed) {
			doClose();
			throw new IOException("Stream is closed by receiver");
		}
			 
		int remaining = len;
		int offset = off;
		while (remaining > 0) {
			if (buffer.payload.remaining() < SizeOf.CHAR) {
				send();
			}

			int size = Math.min(remaining, buffer.payload.remaining() / SizeOf.CHAR);
			buffer.payload.put(array, offset, size);
			offset += size;
			remaining -= size;
		}		
		startupSend();
	}

	@Override
	public void writeArray(double[] array, int off, int len)
			throws IOException {
		if(closed) {
			throw new IOException("Stream is closed");
		}
		if(receiverClosed) {
			doClose();
			throw new IOException("Stream is closed by receiver");
		}
			 
		int remaining = len;
		int offset = off;
		while (remaining > 0) {
			if (buffer.payload.remaining() < SizeOf.DOUBLE) {
				send();
			}

			int size = Math.min(remaining, buffer.payload.remaining() / SizeOf.DOUBLE);
			buffer.payload.put(array, offset, size);
			offset += size;
			remaining -= size;
		}
		startupSend();
	}

	@Override
	public void writeArray(float[] array, int off, int len) throws IOException {
		if(closed) {
			throw new IOException("Stream is closed");
		}
		if(receiverClosed) {
			doClose();
			throw new IOException("Stream is closed by receiver");
		}
			 
		int remaining = len;
		int offset = off;
		while (remaining > 0) {
			if (buffer.payload.remaining() < SizeOf.FLOAT) {
				send();
			}

			int size = Math.min(remaining, buffer.payload.remaining() / SizeOf.FLOAT);
			buffer.payload.put(array, offset, size);
			offset += size;
			remaining -= size;
		}
		startupSend();
	}

	@Override
	public void writeArray(int[] array, int off, int len) throws IOException {
		if(closed) {
			throw new IOException("Stream is closed");
		}
		if(receiverClosed) {
			doClose();
			throw new IOException("Stream is closed by receiver");
		}
			 
		int remaining = len;
		int offset = off;
		while (remaining > 0) {
			if (buffer.payload.remaining() < SizeOf.INT) {
				send();
			}

			int size = Math.min(remaining, buffer.payload.remaining() / SizeOf.INT);
			buffer.payload.put(array, offset, size);
			offset += size;
			remaining -= size;
		}
		startupSend();
	}

	@Override
	public void writeArray(long[] array, int off, int len) throws IOException {
		if(closed) {
			throw new IOException("Stream is closed");
		}
		if(receiverClosed) {
			doClose();
			throw new IOException("Stream is closed by receiver");
		}
			 
		int remaining = len;
		int offset = off;
		while (remaining > 0) {
			if (buffer.payload.remaining() < SizeOf.LONG) {
				send();
			}

			int size = Math.min(remaining, buffer.payload.remaining() / SizeOf.LONG);
			buffer.payload.put(array, offset, size);
			offset += size;
			remaining -= size;
		}
		startupSend();
	}

	@Override
	public void writeArray(short[] array, int off, int len) throws IOException {
		if(closed) {
			throw new IOException("Stream is closed");
		}
		if(receiverClosed) {
			doClose();
			throw new IOException("Stream is closed by receiver");
		}
			 
		int remaining = len;
		int offset = off;
		while (remaining > 0) {
			if (buffer.payload.remaining() < SizeOf.SHORT) {
				send();
			}

			int size = Math.min(remaining, buffer.payload.remaining() / SizeOf.SHORT);
			buffer.payload.put(array, offset, size);
			offset += size;
			remaining -= size;
		}
		startupSend();
	}

	@Override
	public void writeBoolean(boolean value) throws IOException {
		if(true) {
			writeByte((byte)1);
		} else {
			writeByte((byte)0);
		}
	}

	@Override
	public void writeChar(char value) throws IOException {
		if(closed) {
			throw new IOException("Stream is closed");
		}
		if(receiverClosed) {
			doClose();
			throw new IOException("Stream is closed by receiver");
		}
		while(buffer.remaining() < SizeOf.CHAR) {
			send();
			if(closed) {
				throw new IOException("Stream is closed");
			}
			if(receiverClosed) {
				doClose();
				throw new IOException("Stream is closed by receiver");
			}
		}
		buffer.payload.putChar(value);
		startupSend();
	}

	@Override
	public void writeDouble(double value) throws IOException {
		if(closed) {
			throw new IOException("Stream is closed");
		}
		if(receiverClosed) {
			doClose();
			throw new IOException("Stream is closed by receiver");
		}
		while(buffer.remaining() < SizeOf.DOUBLE) {
			send();
			if(closed) {
				throw new IOException("Stream is closed");
			}
			if(receiverClosed) {
				doClose();
				throw new IOException("Stream is closed by receiver");
			}
		}
		buffer.payload.putDouble(value);
		startupSend();
	}

	@Override
	public void writeFloat(float value) throws IOException {
		if(closed) {
			throw new IOException("Stream is closed");
		}
		if(receiverClosed) {
			doClose();
			throw new IOException("Stream is closed by receiver");
		}
		while(buffer.remaining() < SizeOf.FLOAT) {
			send();
			if(closed) {
				throw new IOException("Stream is closed");
			}
			if(receiverClosed) {
				doClose();
				throw new IOException("Stream is closed by receiver");
			}
		}
		buffer.payload.putFloat(value);
		startupSend();
	}

	@Override
	public void writeInt(int value) throws IOException {
		if(closed) {
			throw new IOException("Stream is closed");
		}
		if(receiverClosed) {
			doClose();
			throw new IOException("Stream is closed by receiver");
		}
		while(buffer.remaining() < SizeOf.INT) {
			send();
			if(closed) {
				throw new IOException("Stream is closed");
			}
			if(receiverClosed) {
				doClose();
				throw new IOException("Stream is closed by receiver");
			}
		}
		buffer.payload.putInt(value);
		startupSend();
	}

	@Override
	public void writeLong(long value) throws IOException {
		if(closed) {
			throw new IOException("Stream is closed");
		}
		if(receiverClosed) {
			doClose();
			throw new IOException("Stream is closed by receiver");
		}
		while(buffer.remaining() < SizeOf.LONG) {
			send();
			if(closed) {
				throw new IOException("Stream is closed");
			}
			if(receiverClosed) {
				doClose();
				throw new IOException("Stream is closed by receiver");
			}
		}
		buffer.payload.putLong(value);
		startupSend();
	}

	@Override
	public void writeShort(short value) throws IOException {
		if(closed) {
			throw new IOException("Stream is closed");
		}
		if(receiverClosed) {
			doClose();
			throw new IOException("Stream is closed by receiver");
		}
		while(buffer.remaining() < SizeOf.SHORT) {
			send();
			if(closed) {
				throw new IOException("Stream is closed");
			}
			if(receiverClosed) {
				doClose();
				throw new IOException("Stream is closed by receiver");
			}
		}
		buffer.payload.putShort(value);
		startupSend();
	}
	
	void startupSend() throws IOException {
//		if(flushed && buffer.payload.position() > Config.START_BUFFER_SIZE) {
//		if(buffer.payload.position() > Config.START_BUFFER_SIZE) {
//			send();
//		}
	}
	
}
