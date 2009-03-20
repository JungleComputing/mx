package mxio;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class SendBuffer implements Config {

	static final int BUFFER_CACHE_SIZE = 128;

//	static LinkedBlockingDeque<SendBuffer> cache = new LinkedBlockingDeque<SendBuffer>(BUFFER_CACHE_SIZE);
	static SendBuffer[] cache = new SendBuffer[BUFFER_CACHE_SIZE];
	static int current = 0;
	static ReentrantLock lock = new ReentrantLock();

	private static final Logger logger = LoggerFactory
    .getLogger(SendBuffer.class);

	/**
	 * Static method to get a sendbuffer out of the cache
	 */
	static SendBuffer get() {
		SendBuffer result = null;
		lock.lock();
		if(current != 0) {
			result = cache[current-1];
			current--;
		}
		lock.unlock();
//		SendBuffer result = cache.pollLast();
		if (result != null) {
			if (logger.isInfoEnabled()) {
				logger.info("SendBuffer: got empty buffer from cache");
			}
			result.clear();
			return result;
		}
		if (logger.isInfoEnabled()) {
			logger.info("SendBuffer: got new empty buffer");
		}
		return new SendBuffer();
	}

	/**
	 * static method to put a buffer in the cache
	 */
	static void recycle(SendBuffer buffer) {
		if (buffer.parent == null) {
			if (buffer.copies != 0) {
				buffer.copies--;
				// throw new Error("tried to recycle buffer with children!");
				return;
			} else {
				lock.lock();	
//				if(!cache.offerLast(buffer)) {
				if(current >= BUFFER_CACHE_SIZE) {
					if (logger.isInfoEnabled()) {
						logger.info("SendBuffer: cache full"
								+ " upon recycling buffer, throwing away");
					}
				} else {
					cache[current] = buffer;
					current++;
					if (logger.isInfoEnabled()) {
						logger.info("SendBuffer: recycled buffer");
					}
				}
				lock.unlock();
			}
		} else {
			if (logger.isInfoEnabled()) {
				logger.info("SendBuffer: recycling child buffer");
			}
			buffer.parent.copies--;
			if (buffer.parent.copies < 0) {
				buffer.parent.copies = 0;
				lock.lock();
//				if(!cache.offerLast(buffer.parent)) {
				if(current >= BUFFER_CACHE_SIZE) {
					if (logger.isInfoEnabled()) {
						logger.info("SendBuffer: cache full"
								+ " upon recycling parent of child buffer,"
								+ " throwing away");
					}
				} else {
					cache[current] = buffer;
					current++;
					if (logger.isInfoEnabled()) {
						logger.info("SendBuffer: recycled parent buffer");
					}
				}
				lock.unlock();
			}
		}
	}

	/**
	 * create copies of a buffer, records how may copies are made so far
	 */
	static SendBuffer[] replicate(SendBuffer original, int copies) {
		SendBuffer[] result = new SendBuffer[copies];

		for (int i = 0; i < copies; i++) {
			result[i] = new SendBuffer(original);
		}
		original.copies += copies;
		if(logger.isDebugEnabled()) {
			logger.debug("" + copies + " Copies of the SendBuffer created");
		}

		return result;
	}

	// number of copies that exist of this buffer
	private int copies = 0;

	// original buffer this buffer is a copy of (if applicable)
	SendBuffer parent = null;

	ByteBuffer payload;
	ByteBuffer header;

	SendBuffer() {
		ByteOrder order = ByteOrder.nativeOrder();

		payload = ByteBuffer.allocateDirect(Config.BUFFER_SIZE).order(
				order);
		header = ByteBuffer.allocateDirect(Config.SIZEOF_HEADER).order(
				order);

		// put the byte order in the first byte of the header
		if (order == ByteOrder.BIG_ENDIAN) {
			header.put(0, (byte) 1);
		} else {
			header.put(0, (byte) 0);
		}

	}

	/**
	 * Copy constructor. Acutally only copies byteBuffers;
	 */
	SendBuffer(SendBuffer parent) {
		ByteOrder order = ByteOrder.nativeOrder();

		this.parent = parent;
		payload = parent.payload.duplicate();
		header = ByteBuffer.allocateDirect(Config.SIZEOF_HEADER).order(
				order);
//		 put the byte order in the first byte of the header
		if (order == ByteOrder.BIG_ENDIAN) {
			header.put(0, (byte) 1);
		} else {
			header.put(0, (byte) 0);
		}
	}


	/**
	 * 
	 * 
	 * /** Resets a buffer as though it's a newly created buffer. Sets the
	 * sequencenr to a new value
	 */
	void clear() {
		setPort(0);
		payload.clear();
		header.clear();
	}

	/**
	 * Make a (partially) filled buffer ready for sending
	 */
	void flip() {
		payload.flip();

		if (logger.isDebugEnabled()) {
			logger.debug("flipping buffer, sending: b[" + payload.remaining()
					+ "] total size: " + (payload.remaining() + Config.SIZEOF_HEADER));
		}
	}

	void setPort(int port) {
		header.putInt(Config.PORT_BYTE, port);
	}

	int getPort(int port) {
		return header.getInt(Config.PORT_BYTE);
	}

	/**
	 * set a mark on Byte Buffer
	 */
	void mark() {
		payload.mark();
	}

	/**
	 * reset Byte Buffer
	 */
	void reset() {
		payload.reset();
	}

	/**
	 * returns the number of remaining bytes in the bytebuffers
	 */
	long remaining() {
		return payload.remaining();
	}

	boolean isEmpty() {
		return payload.position() == 0;
	}

	boolean hasRemaining() {
		return payload.hasRemaining();
	}
	
	/**
	 * returns the number of remaining bytes in the bytebuffers
	 */
	long msgSize() {
		return payload.remaining() + header.capacity();
	}
}
