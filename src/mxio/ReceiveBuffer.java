package mxio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ReceiveBuffer {


	static final int BUFFER_CACHE_SIZE = 128;

//	static java.util.concurrent.LinkedBlockingDeque<ReceiveBuffer> cache = new LinkedBlockingDeque<ReceiveBuffer>(BUFFER_CACHE_SIZE);
	
	static ReceiveBuffer[] cache = new ReceiveBuffer[BUFFER_CACHE_SIZE];
	static int current = 0;
	static ReentrantLock lock = new ReentrantLock();
	
	
	private static final Logger logger = LoggerFactory
    .getLogger(ReceiveBuffer.class);
	
	/**
	 * Static method to get a sendbuffer out of the cache
	 */
	static ReceiveBuffer get() {
//		ReceiveBuffer result = cache.pollLast();
		ReceiveBuffer result = null;
		lock.lock();
		if(current != 0) {
			result = cache[current-1];
			current--;
		}
		lock.unlock();
		
		if (result != null) {
			if (logger.isInfoEnabled()) {
				logger.info("ReceiveBuffer: got empty buffer from cache");
			}
			result.clear();
			return result;
		}
		if (logger.isInfoEnabled()) {
			logger.info("ReceiveBuffer: got new empty buffer");
		}
		return new ReceiveBuffer();
	}

	/**
	 * static method to put a buffer in the cache
	 */
	static void recycle(ReceiveBuffer buffer) {
		lock.lock();
//		if(!cache.offerLast(buffer)) {
		if(current >= BUFFER_CACHE_SIZE) {
			if (logger.isInfoEnabled()) {
				logger.info("ReceiveBuffer: cache full"
						+ " upon recycling buffer, throwing away");
			}
			buffer.destroy();
		} else {
			cache[current] = buffer;
			current++;
			if (logger.isInfoEnabled()) {
				logger.info("ReceiveBuffer: recycled buffer");
			}
		}
		lock.unlock();
	} 

	/**
	 * Buffer used for holding data. It contains "currently in use" (by
	 * the user) data, "not yet used" data (received but not given to user yet)
	 * and "empty space"
	 */
	private ByteBuffer buffer;

	private int port = 0;

	private int myHandle = 0;
	private int endpointNumber = 0;
	
	int postStatus;
	static final int IDLE = 0, POSTED = 1, FINISHED = 2;

	ReceiveBuffer() {
		myHandle =  JavaMx.handles.getHandle();
		buffer = ByteBuffer.allocateDirect(Config.SIZEOF_HEADER + Config.BUFFER_SIZE);
		buffer.clear();
	}

	void post(int endpointNumber, long matchData, long matchMask) throws IOException {
		if(postStatus != IDLE) {
			// Buffer is already posted, abort
			if(postStatus == POSTED) {
				// buffer is already in a consistent state, so nothing is wrong if we just return here
				if (logger.isDebugEnabled()) {
					logger.debug("Reposting a posted buffer: ignore");
				}				
				return;
			} else {
				throw new IOException("Buffer already received a message");
			}
		}

		this.endpointNumber = endpointNumber;

		try {
			JavaMx.recv(buffer, 0, buffer.capacity(), endpointNumber, myHandle, matchData, matchMask);
			postStatus = POSTED;
		} catch (MxException e) {
			// TODO handle this?
			throw e;
		} 
	}

	/**
	 * @return true when message is canceled, false when message already has arrived and request still has to be finished
	 *  
	 */
	boolean cancel() {
		if(postStatus != POSTED) {
			return false;
		}
		//TODO hack: return JavaMx.cancel(endpointNumber, myHandle);
		JavaMx.forget(endpointNumber, myHandle);
		return true;
	}

	boolean finish(boolean poll) throws IOException {
		return finish(0, poll);
	}

//	long polls = 0;
//	long ntime = 0;
	
	boolean finish(long timeout, boolean poll) throws IOException {
		if(postStatus != POSTED) {
			if(postStatus == FINISHED) {
				if (logger.isDebugEnabled()) {
					logger.debug("buffer already finished");
				}
				// message already delivered, just return here
				return true;
			} else {
				throw new IOException("Buffer not posted yet");
			}
		}
		
		ByteOrder receivedOrder;
		try {
			int msgSize = -1;
			
			if(poll) {
				int i = 0;
//				long nt = - System.nanoTime();
				while(msgSize < 0 && i < Config.POLLS) {
					msgSize = JavaMx.test(endpointNumber, myHandle);
					i++;
				}
//				nt += System.nanoTime();
//				polls +=i;
//				ntime += nt;
//				if(polls > 1000000) {
//					System.out.println("RB: " + polls + " polls took " + ((double)ntime/1000000) + " millis");
//					ntime = polls = 0;
//				}
			}
			if(msgSize < 0) {
//				System.out.println("poll miss");
				msgSize = JavaMx.wait(endpointNumber, myHandle, timeout);
				if(msgSize < 0) {
					// Timeout
					return false;
				}
			}

			postStatus = FINISHED;			
			buffer.position(0).limit(msgSize);

			if (logger.isDebugEnabled()) {
				logger.debug("Message of " + msgSize + " bytes received.");
			}
		} catch (MxException e) {
			// FIXME Auto-generated catch block
			e.printStackTrace();
			throw e;
		}

		// get byte order out of first byte in header
		if (buffer.get(Config.BYTEORDER_BYTE) == ((byte) 1)) {
			receivedOrder = ByteOrder.BIG_ENDIAN;
		} else {
			receivedOrder = ByteOrder.LITTLE_ENDIAN;
		}
		buffer.order(receivedOrder);

		port = buffer.getInt(Config.PORT_BYTE);
	
		buffer.position(Config.SIZEOF_HEADER);

		return true;
	}

	int remaining() {
		return (buffer.remaining());
	}

	byte readByte() throws IOException {
		byte result;

		result = buffer.get();

		return result;
	}


	int readArray(byte ref[], int off, int len) throws IOException {
		int remaining = buffer.remaining();

		if (len <= remaining) {
			buffer.get(ref, off, len);
			return len;
		} else {
			buffer.get(ref, off, remaining);
			return remaining;
		}
	}

	void clear() {
		postStatus = IDLE;
		buffer.clear().flip(); // buffer contains no data
		port = 0;
	}

	int handle() {
		return myHandle;
	}

	int port() {
		return port;
	}

	private void destroy() {
		JavaMx.handles.releaseHandle(myHandle);
	}

}
