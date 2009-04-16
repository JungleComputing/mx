package mxio;

import java.nio.ByteOrder;

import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class MxSendBuffer implements Config {

	//	static LinkedBlockingDeque<SendBuffer> cache = new LinkedBlockingDeque<SendBuffer>(BUFFER_CACHE_SIZE);
	static MxSendBuffer[] cache = new MxSendBuffer[BUFFER_CACHE_SIZE];
	static int current = 0;
	static ReentrantLock lock = new ReentrantLock();

	private static final Logger logger = LoggerFactory
	.getLogger(MxSendBuffer.class);

	/**
	 * Static method to get a sendbuffer out of the cache
	 */
	static MxSendBuffer get() {
		MxSendBuffer result = null;
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
		result = new MxSendBuffer();
		if (logger.isInfoEnabled()) {
			logger.info("SendBuffer: got new empty buffer");
		}
		return result;
	}

	/**
	 * static method to put a buffer in the cache
	 */
	static void recycle(MxSendBuffer buffer) {
		if (buffer.parent == null) {
			synchronized(buffer) {
				if (buffer.copies > 0) {
					buffer.copies--;
					if (logger.isInfoEnabled()) {
						logger.info("SendBuffer: Children of parent buffer still alive");
					}
					return;
				} else if (buffer.copies < 0) {
					throw new Error("recycled buffer more than once!");
				} else {
					lock.lock();	
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
			}
		} else {
			synchronized(buffer.parent) {
				buffer.parent.copies--;
				if (buffer.parent.copies == -1) {
					buffer.parent.copies = 0;
					lock.lock();
					if(current >= BUFFER_CACHE_SIZE) {
						if (logger.isInfoEnabled()) {
							logger.info("SendBuffer: cache full"
									+ " upon recycling parent of child buffer,"
									+ " throwing away");
						}
					} else {
						cache[current] = buffer.parent;
						current++;
						if (logger.isInfoEnabled()) {
							logger.info("SendBuffer: recycled parent buffer");
						}
					}
					lock.unlock();
				}
				if (buffer.parent.copies < -1) {
					throw new Error("recycled parent buffer too often!");
				}
			}
		}
	}

	/**
	 * create copies of a buffer, records how may copies are made so far
	 */
	static MxSendBuffer[] replicate(MxSendBuffer original, int copies) {
		MxSendBuffer[] result = new MxSendBuffer[copies];

		for (int i = 0; i < copies; i++) {
			result[i] = new MxSendBuffer(original);
		}
		original.copies += copies;
		if(logger.isInfoEnabled()) {
			logger.info("" + copies + " Copies of the SendBuffer created");
		}

		return result;
	}

	// number of copies that exist of this buffer
	private int copies = 0;

	// original buffer this buffer is a copy of (if applicable)
	MxSendBuffer parent = null;

	MxIOBuffer payload;
	MxIOBuffer header;

	MxSendBuffer() {
		ByteOrder order = ByteOrder.nativeOrder();

		payload = new MxIOBuffer(Config.BUFFER_SIZE).order(
				order);
		header = new MxIOBuffer(Config.SIZEOF_HEADER).order(
				order);

		// put the byte order in the first byte of the header
		if (order == ByteOrder.BIG_ENDIAN) {
			header.put(0, (byte) 1);
		} else {
			header.put(0, (byte) 0);
		}

	}

	/**
	 * Copy constructor. Actually only copies byteBuffers;
	 */
	MxSendBuffer(MxSendBuffer original) {
		ByteOrder order = ByteOrder.nativeOrder();

		parent = original;
		payload = parent.payload.duplicate();
		header = new MxIOBuffer(Config.SIZEOF_HEADER).order(
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
	int remaining() {
		return payload.remaining();
	}

	int position() {
		return payload.position();
	}
	
	void setLimit(int limit) {
		payload.limit(limit);
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
