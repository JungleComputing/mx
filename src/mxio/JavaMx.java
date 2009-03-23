package mxio;

import java.nio.ByteBuffer;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class JavaMx {

	static final class HandleManager {

		private static final Logger logger = LoggerFactory
		.getLogger(HandleManager.class);

		int size, blockSize, maxBlocks, activeHandles;
		boolean[] inUse;

		/**
		 * @param blockSize The size of the memory blocks used
		 * @param maxBlocks The maximum number of blocks
		 * @throws MxException
		 */
		private HandleManager(int blockSize, int maxBlocks) throws MxException {			
			this.size = 0;
			this.blockSize = blockSize;
			this.maxBlocks = maxBlocks;
			this.activeHandles = 0;
			this.inUse = new boolean[size];
			if (init(blockSize, maxBlocks) == false) {
				throw new MxException("HandleManager: could not initialize Handles.");
			}
		}

		private native boolean init(int blockSize, int maxBlocks);
		private native boolean addBlock();

		/**
		 * Allocates a request handle in native code
		 * @return the handle identifier
		 */
		protected synchronized int getHandle() {
			if(activeHandles == size) {
				//Size up handle array, also in C code
				if(addBlock() == false) {
					//TODO exception
					System.exit(1);
				}
				if (logger.isDebugEnabled()) {
					logger.debug("Block added");
				}
				size += blockSize;
				boolean[] temp = new boolean[size];
				for(int i = 0; i < inUse.length; i++) {
					temp[i] = inUse[i];
				}
				inUse = temp;
			}

			int handle = 0;
			while(inUse[handle]) {
				handle++;
			}
			inUse[handle] = true;
			activeHandles++;
			if (logger.isDebugEnabled()) {
				logger.debug("Handle " + handle + " distributed");
			}

			return handle;
		}

		/**
		 * release a handle that is not in use anymore
		 * @param handle the handle 
		 */
		protected synchronized void releaseHandle(int handle) {
			if(!inUse[handle]) {
				//FIXME exception
				throw new Error("handle already released!");
			}
			inUse[handle] = false;
			activeHandles--;
			if (logger.isDebugEnabled()) {
				logger.debug("Handle " + handle + " released");
			}
		}

	}

	static final class LinkManager {
		private static final Logger logger = LoggerFactory
		.getLogger(LinkManager.class);
		
		int size, blockSize, maxBlocks, activeTargets;
		boolean[] inUse;

		/**
		 * @param blockSize The size of the memory blocks used
		 * @param maxBlocks The maximum number of blocks
		 * @throws MxException
		 */
		private LinkManager(int blockSize, int maxBlocks) throws MxException {
			this.size = 0;
			this.blockSize = blockSize;
			this.maxBlocks = maxBlocks;
			this.activeTargets = 0;
			inUse = new boolean[size];
			if (init(blockSize, maxBlocks) == false) {
				throw new MxException("LinkManager: could not initialize Links.");
			}
			// init calls to C library
		}

		private native boolean init(int blockSize, int maxBlocks);
		private native boolean addBlock();

		/**
		 * Allocates a link data structure in native code
		 * @return the link identifier
		 */
		protected synchronized int getLink() {
			if(activeTargets == size) {
				//Size up target array, also in C code
				if(addBlock() == false) {
					//TODO exception
					System.exit(1);
				}
				size += blockSize;
				boolean[] temp = new boolean[size];
				for(int i = 0; i < inUse.length; i++) {
					temp[i] = inUse[i];
				}
				inUse = temp;
			}

			int target = 0;
			while(inUse[target] ) {
				target++;
			}
			inUse[target] = true;
			activeTargets++;
			if (logger.isDebugEnabled()) {
				logger.debug("Link " + target + " distributed");
			}
			return target;
		}

		/**
		 * releases a link
		 * @param the link that can be released
		 */
		protected synchronized void releaseLink(int target) {
			inUse[target] = false;
			activeTargets--;
			if (logger.isDebugEnabled()) {
				logger.debug("Link " + target + " released");
			}
		}
	}

	private static final Logger logger = LoggerFactory
	.getLogger(JavaMx.class);

	public static boolean initialized;
	static HandleManager handles;
	static LinkManager links;

	static {
		try {
			System.loadLibrary("myriexpress");
			System.loadLibrary("javamx");
			if (logger.isDebugEnabled()) {
				logger.debug("init: libraries found");
			}
			initialized = init();
			if(!initialized) {
				if (logger.isDebugEnabled()) {
					logger.debug("Initializing JavaMX library failed.");
				}
			} else {
				//TODO choose nice values here
				//FIXME first argument of 128 for both resulted in errors on 80 nodes
				// Something wrong in mem management???
				handles = new HandleManager(1024, 128*1024);
				links = new LinkManager(512, 2*1024);
			}
		} catch (Throwable e) {
			if (logger.isDebugEnabled()) {
				logger.debug("Error initializing JavaMx: " + e.getMessage());
			}
			initialized = false;
		}	
	}

	/**
	 * Initializes the JavaMx library.
	 * @return True when successful.
	 */
	static native boolean init() throws MxException;

	/**
	 * Stops the library.
	 * @return always true?
	 */
	static native boolean deInit();

	/**
	 * Opens a new endpoint.
	 * @param filter The filter that is used for this endpoint.
	 * @return The endpoint identifier.
	 */
	static native int newEndpoint(int filter); 

	/**
	 * Closes an endpoint.
	 * @param endpointNumber the endpoint number of the endpoint that will be closed.
	 */
	static native void closeEndpoint(int endpointNumber);

	/**
	 * @param endpointNumber The endpoint.
	 * @return The NIC ID of the NIC the endpoint located at.
	 */
	static native long getMyNicId(int endpointNumber);

	/**
	 * @param endpointId The JavaMx endpoint ID.
	 * @return The endpoint number that is used in the mx library. This can be different from the identifier used in JavaMx. 
	 */
	static native int getMyEndpointId(int endpointNumber);

	/**
	 * Gets the NIC identifier of a host by its name.
	 * @param name The name to resolve.
	 * @return The NIC identifier.
	 */
	static native long getNicId(String name);

	/**
	 * Sets up a connection for writing to a remote endpoint. Connections are one-way in JavaMx. 
	 * @param endpointNumber The local endpoint.
	 * @param link The identifier of the link data structure which can be used for this connection.
	 * @param targetNicId The NIC ID of the receiver side.
	 * @param targetEndpoint The (native) endpoint id of the receiver side.
	 * @param filter The filter which should be used for this connection.
	 * @return True when successful, false if something went wrong.
	 */
	static native boolean connect(int endpointNumber, int link, long targetNicId, int targetEndpoint, int filter) throws MxException;

	/**
	 * Sets up a connection for writing to a remote endpoint. Connections are one-way in JavaMx. 
	 * @param endpointNumber The local endpoint.
	 * @param link The identifier of the link data structure which can be used for this connection.
	 * @param targetNicId The NIC ID of the receiver side.
	 * @param targetEndpoint The (native) endpoint id of the receiver side.
	 * @param filter The filter which should be used for this connection.
	 * @param timeout The timeout in milliseconds.
	 * @return true when successful, false when a timeout occurs, or on any other error.
	 */
	static native boolean connect(int endpointNumber, int link, long targetNicId, int targetEndpoint, int filter, long timeout) throws MxException;

	/**
	 * Closes the connection.
	 * @param link The link datastructure of the connection that can be closed.
	 * @return true when succesful, false if the link does not exist (anymore).
	 */
	static native boolean disconnect(int link);

	/**
	 * Initiates an unreliable message transfer over a link. This request is successful when the message transfer is initiated successfully.
	 * @param buffer A buffer containing the message
	 * @param offset The offset of the message in the buffer.
	 * @param msgSize The size of the message in bytes.
	 * @param endpointNumber The endpoint that will be used to send the message. 
	 * @param link The link over which the message will be sent.
	 * @param handle The request handle that can be used for this operation.
	 * @param matchData The matching data.
	 */
	static native void send(ByteBuffer buffer, int offset, int msgSize, int endpointNumber, int link, int handle, long matchData);

	/**
	 * Initiates a reliable message transfer over a link. This request is succesful when the message is received correctly.
	 * @param buffer A buffer containing the message.
	 * @param offset The offset of the message in the buffer.
	 * @param msgSize The size of the message in bytes.
	 * @param endpointNumber The endpoint that will be used to send the message. 
	 * @param link The link over which the message will be sent.
	 * @param handle The handle that can be used for this request.
	 * @param matchData The matching data.
	 */
	static native void sendSynchronous(ByteBuffer buffer, int offset, int msgSize, int endpointNumber, int link, int handle, long matchData);

	/* the same, but with 2 buffers */
	static native void send(
			ByteBuffer header, int headerSize,
			ByteBuffer payload, int payloadSize,			
			int endpointNumber, int link, int handle, long matchData
	);

	static native void sendSynchronous(
			ByteBuffer header, int headerSize,
			ByteBuffer payload, int payloadSize,			
			int endpointNumber, int link, int handle, long matchData
	);

	/**
	 * Receives a message from an endpoint. Only message with correct matching data will be received.
	 * @param buffer The buffer in to which the message will be written.
	 * @param offset The offset in the buffer where the message will be written at.
	 * @param bufsize The maximum number of bytes that can be written to the buffer.
	 * @param endpointNumber The local endpoint at which the message must arrive.
	 * @param handle The handle that can be used for this request.
	 * @param matchData The matching data. Only messages with exactly this matching data will be received by this request.
	 * @throws MxException 
	 */
	static native void recv(ByteBuffer buffer, int offset, int bufsize, int endpointNumber, int handle, long matchData) throws MxException; // returns a handle

	/**
	 * Receives a message from an endpoint. Only message with correct matching data will be received.
	 * @param buffer The buffer in to which the message will be written.
	 * @param offset The offset in the buffer where the message will be written at.
	 * @param bufsize The maximum number of bytes that can be written to the buffer.
	 * @param endpointNumber The local endpoint at which the message must arrive.
	 * @param handle The handle that can be used for this request.
	 * @param matchData The matching data. Only messages with matching data that equals this field after masking with the mask will be received by this request.
	 * @param matchMask The mask applied to the matching data of the message. 
	 * @throws MxException
	 */
	static native void recv(ByteBuffer buffer, int offset, int bufsize, int endpointNumber, int handle, long matchData, long matchMask) throws MxException; // returns a handle

	// TODO implement
	static native void recv(ByteBuffer buffer, int offset, int bufsize,
			ByteBuffer buffer2, int offset2, int bufsize2, int endpointNumber,
			int handle, long matchData) throws MxException;	

	static native void recv(ByteBuffer buffer, int offset, int bufsize,
			ByteBuffer buffer2, int offset2, int bufsize2, int endpointNumber,
			int handle, long matchData, long matchMask) throws MxException;

	/**
	 * Waits for a request to finish.
	 * @param endpointNumber The local endpoint number. 
	 * @param handle The handle of the request to wait for.
	 * @return The message size, or -1 when not successful.
	 * @throws MxException
	 */
	static native int wait(int endpointNumber, int handle) throws MxException; // return message size by success, -1, when unsuccessful

	/**
	 * Waits for a request to finish.
	 * @param endpointNumber The local endpoint number. 
	 * @param handle The handle of the request to wait for.
	 * @param timeout The timeout in milliseconds.
	 * @return The message size, or -1 when not successful.
	 * @throws MxException
	 */
	static native int wait(int endpointNumber, int handle, long timeout) throws MxException; // return message size by success, -1, when unsuccessful

	/**
	 * Tests whether a request is finished.
	 * @param endpointNumber The local endpoint number. 
	 * @param handle The handle of the request to test for.
	 * @return The message size, or -1 when not successful.
	 * @throws MxException
	 */
	static native int test(int endpointNumber, int handle) throws MxException; // return message size by success, -1, when unsuccessful

	/**
	 * Probes for a new message that is ready to be received. This call returns immediately.
	 * @param endpointNumber The local endpoint number.
	 * @param matchData The matching data. Only messages with matching data that equals this field after masking with the mask will be probed for by this request.
	 * @param matchMask The mask applied to the matching data of the message. 
	 * @return the size of the message that can be received, or -1 when there is no message
	 */
	static native int iprobe(int endpointNumber, long matchData, long matchMask);

	/**
	 * Probes for a new message that is ready to be received. Blocks until the timeout expires.
	 * @param endpointNumber The local endpoint number.
	 * @param timeout The timeout in milliseconds, 0 means no timeout.
	 * @param matchData The matching data. Only messages with matching data that equals this field after masking with the mask will be probed for by this request.
	 * @param matchMask The mask applied to the matching data of the message. 
	 * @return the size of the message that can be received, or -1 when there is no message
	 */
	static native int probe(int endpointNumber, long timeout, long matchData, long matchMask);

	/**
	 * Cancels a receive pending request and clean up its resources. Beware: Cannot be used for send() requests.
	 * @param endpointNumber The number of the endpoint we are working on.
	 * @param handle The handle of a request that has to be canceled.
	 * @return True when the request is canceled, false when it was too late to cancel the request. 
	 * In that case, the request still has to be completed by calling test() or wait() 
	 */
	static native boolean cancel(int endpointNumber, int handle);

	/**
	 * Forget a pending request and clean up its resources. Probably works on all kinds of requests
	 * @param endpointNumber The number of the endpoint we are working on.
	 * @param handle The handle of a request that has to be canceled. 
	 */
	static native void forget(int endpointNumber, int handle);

	/**
	 * Wake up all threads that are blocked on the endpoint.
	 * @param endpointNumber The endpoint for which all threads will be waked up.
	 */
	static native void wakeup(int endpointNumber);

	/**
	 * Waits for a message to arrive
	 * @param endpointNumber the endpoint to work on
	 * @return the matching information of the next message, or 0 (Matching.MATCH_NONE) when no message has arrived
	 */
	static native long waitForMessage(int endpointNumber, long timeout, long matchData, long matchMask);

	/**
	 * @param endpointNumber the endpoint to work on
	 * @return the matching information of the next message, or 0 (Matching.MATCH_NONE) when no message has arrived
	 */
	static native long pollForMessage(int endpointNumber, long matchData, long matchMask);

	/**
	 * completes the first request that is finished and matches the given matching information
	 * @param endpointNumber endpointNumber the endpoint to work on
	 * @param timeout The timeout in milliseconds
	 * @param matchData The matching data. Only messages with matching data that equals this field after masking with the mask will be probed for by this request.
	 * @param matchMask The mask applied to the matching data of the message. 
	 * @param matchedConnection A direct ByteBuffer in which the matching information of the received message will be stored
	 * @return the size of the received message, or -1 in case of an timeout
	 */
	static native int select(int endpointNumber, long timeout, long matchData, long matchMask, ByteBuffer matchedConnection);

	/*
	static void send(SendBuffer buffer, int endpointNumber, int link, int handle, long matchData) {
		send(
			buffer.byteBuffers[0], buffer.byteBuffers[0].remaining(),
			buffer.byteBuffers[1], buffer.byteBuffers[1].remaining(),
			buffer.byteBuffers[2], buffer.byteBuffers[2].remaining(),
			buffer.byteBuffers[3], buffer.byteBuffers[3].remaining(),
			buffer.byteBuffers[4], buffer.byteBuffers[4].remaining(),
			buffer.byteBuffers[5], buffer.byteBuffers[5].remaining(),
			buffer.byteBuffers[6], buffer.byteBuffers[6].remaining(),
			buffer.byteBuffers[7], buffer.byteBuffers[7].remaining(),
			buffer.byteBuffers[8], buffer.byteBuffers[8].remaining(),
			endpointNumber, link, handle, matchData
			);
	}
	static void sendSynchronous(SendBuffer buffer, int endpointNumber, int link, int handle, long matchData) {
		sendSynchronous(
			buffer.byteBuffers[0], buffer.byteBuffers[0].remaining(),
			buffer.byteBuffers[1], buffer.byteBuffers[1].remaining(),
			buffer.byteBuffers[2], buffer.byteBuffers[2].remaining(),
			buffer.byteBuffers[3], buffer.byteBuffers[3].remaining(),
			buffer.byteBuffers[4], buffer.byteBuffers[4].remaining(),
			buffer.byteBuffers[5], buffer.byteBuffers[5].remaining(),
			buffer.byteBuffers[6], buffer.byteBuffers[6].remaining(),
			buffer.byteBuffers[7], buffer.byteBuffers[7].remaining(),
			buffer.byteBuffers[8], buffer.byteBuffers[8].remaining(),
			endpointNumber, link, handle, matchData
			);
	}
	 */

	//TODO do not use filters in Java, but move them away to the native code completely ?
	//TODO add endpoint info to links in LinkManager?
}
