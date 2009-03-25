package mxio;

import java.io.IOException;
import java.nio.BufferOverflowException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class OutputStream extends java.io.OutputStream {
	//FIXME prepare for use in multicastStream
	public static final int BUFFER_SIZE = mxio.Config.BUFFER_SIZE;
	
	private static final Logger logger = LoggerFactory
    .getLogger(OutputStream.class);

	private SendBuffer buffer;

	long count = 0;
	int bytesInBuffer = 0;

	boolean closed = false;
	boolean receiverClosed = false;

	MulticastOutputStream mcStream = null;

	protected OutputStream() {
		buffer = SendBuffer.get();
	}

	public boolean isOpen() {
		return !closed;
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
					count += doSend(buffer);
					doFlush();
				} else {
					SendBuffer.recycle(buffer);
					doFlush();
				}
				buffer = null;
			} catch (IOException e1) {
				// ignore
			}
		}
		try {
			doClose();
		} catch (IOException e) {
			// ignore
		}
	}

	final void addToMulticast(MulticastOutputStream stream) throws IOException {
		if(mcStream != null) {
			// can only be part of 1 mcStream
			throw new IOException("Already in MulticastOutputStream");
		}
		
		flush(); // throws an IOException

		SendBuffer.recycle(buffer);
		buffer = null;
		mcStream = stream;
	}

	final void removeFromMulticast() {
		if(mcStream == null) {
			//huh, cannot happen. At the other hand, still nothing is wrong with this channel 
			return;
		}
		buffer = SendBuffer.get();
	}

	protected void send() throws IOException {
		if (buffer.isEmpty()) {
			return;
		}
		buffer.flip();
		count += doSend(buffer);

		// get a new buffer
		buffer = SendBuffer.get();
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
	}

	public int bufferSize() {
		return 0;
	}

	abstract long doSend(SendBuffer buffer) throws IOException;

	abstract void doFlush() throws IOException;

	abstract void doClose() throws IOException;

	public long bytesWritten() {
		return count;
	}

	public void resetBytesWritten() {
		count = 0;
	}

	public void writeByte(byte value) throws IOException {
		if(closed) {
			throw new IOException("Stream is closed");
		}
		if(receiverClosed) {
			doClose();
			throw new IOException("Stream is closed by receiver");
		}
		
		try {
			buffer.payload.put(value);
		} catch (BufferOverflowException e) {
			// buffer was full, send
			send();
			// and try again
			buffer.payload.put(value);
		}
	}

	public void writeArray(byte[] array, int off, int len) throws IOException {
		if(closed) {
			throw new IOException("Stream is closed");
		}
		if(receiverClosed) {
			doClose();
			throw new IOException("Stream is closed by receiver");
		}
			 
		try {
			buffer.payload.put(array, off, len);
		} catch (BufferOverflowException e) {
			// do this the hard way

			while (len > 0) {
				if (!buffer.payload.hasRemaining()) {
					send();
				}

				int size = Math.min(len, buffer.payload.remaining());
				buffer.payload.put(array, off, size);
				off += size;
				len -= size;
			}
		}
	}

	public void write(int value) throws IOException {
		writeByte((byte) value);
	}
	
	public void write(byte[] b, int off, int len) throws IOException {
		writeArray(b, off, len);
	}
	
	public void write(byte[] b) throws IOException {
		write(b, 0, b.length);
	}

}
