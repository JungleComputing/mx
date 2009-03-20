package mxio;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutputStreamImpl extends OutputStream {

	private static final Logger logger = LoggerFactory
    .getLogger(OutputStreamImpl.class);

	private MxAddress target;
	private int endpointNumber;
	private long matchData;
	private int myLink;

	private class FlushQueue {
		int[] handles;
		SendBuffer[] queue;
		
		int head;
		int elements;
		
		int size;
		private boolean destroyed = false;
		
		FlushQueue(int size) {
			head = elements = 0;
			this.size = size;
			queue = new SendBuffer[size];
			handles = new int[size];
			for (int i = 0; i < size; i++) {
				handles[i] = JavaMx.handles.getHandle();
			}
		}

		protected void finalize() {
			destroy();
		}

		SendBuffer flushHead() throws MxException {
			if (elements == 0) {
				return null;
			}
			
			SendBuffer buf = queue[head];
			
			int msgSize = -1;
			int i = 0;
			while(msgSize < 0 && i < Config.POLLS) {
				msgSize = JavaMx.test(endpointNumber, handles[head]);
				i++;
			}
			while(msgSize < 0) {
				msgSize = JavaMx.wait(endpointNumber, handles[head]);
			}
			
			head = (head+1) % size;
			elements--;
			if(msgSize == -1) {
			//	error
				throw new Error("send error 1b");
			}
			if (msgSize != buf.msgSize() ) {
				//error
				throw new Error("send error 2b");
			}
			return buf;
		}
		
		boolean doSend(SendBuffer buffer) throws MxException {
			if( elements == size) {
				return false;
			}
			
			int tail = (head + elements) % size;
			queue[tail] = buffer;
			elements++;
						
			JavaMx.send(buffer.header, buffer.header.capacity(), buffer.payload, buffer.payload.remaining(), endpointNumber, 
					myLink, handles[tail], matchData);
			return true;
		}

		boolean isEmpty() {
			return elements == 0;
		}
		
		void destroy() {
			if(!destroyed ) {
				for (int i = head; i < elements; i = (i+1)%size) {
					JavaMx.forget(endpointNumber, handles[i]);
				}
				
				for (int i = 0; i < size; i++) {
					JavaMx.handles.releaseHandle(handles[i]);
				}
				handles = null;
				queue = null;
				elements = 0;
				size = 0;
			}
			destroyed = true;
		}
	}
	
	private FlushQueue flushQueue;
	
	private int port;
	
	protected OutputStreamImpl(MxSocket socket, int endpointNumber, int link,
			long matchData, MxAddress target) {
		super();
		
		flushQueue = new FlushQueue(Config.FLUSH_QUEUE_SIZE);
		this.endpointNumber = endpointNumber;
		this.matchData = matchData;
		this.target = target;
		this.port = Matching.getPort(matchData);
		myLink = link;
	}
	
	long doSend(SendBuffer buffer) throws IOException {
		buffer.setPort(port);
		long size = buffer.remaining();
		
		while(!flushQueue.doSend(buffer)) {
			SendBuffer flushedBuf = flushHead();
			if(flushedBuf != null) {
				SendBuffer.recycle(flushedBuf);
			}
		}
		
		return size;
	}
		
	void doFlush() throws IOException {
		SendBuffer buffer;	
		
		while(!flushQueue.isEmpty()) {
			buffer = flushHead();
			if(buffer != null) {
				SendBuffer.recycle(buffer);
			}
		}
	}
	
	SendBuffer flushHead() throws MxException {
		return flushQueue.flushHead();
	}
	
	
	
	void doClose() throws IOException {
		closed = true;
		if(!receiverClosed) {
			sendDisconnectMessage();
		}
		flushQueue.destroy();
	}
	
	protected void receiverClosedConnection() {
		if (closed || receiverClosed) {
			return;
		}
		receiverClosed = true;
		// TODO forget messages that are in transit?
		try {
			doClose();
		} catch (IOException e) {
			// ignore
		}
	}
	
//	 My OutputStream closes, notify the receiver
	private void sendDisconnectMessage() {
		//TODO move this to the socket??
		int handle = JavaMx.handles.getHandle();
		long matchData = Matching.setProtocol(this.matchData,
				Matching.PROTOCOL_DISCONNECT);
		JavaMx.send(null, 0, 0, endpointNumber, myLink, handle,
				matchData);
		try {
			if(JavaMx.wait(endpointNumber, handle, 1000) == -1) {
				JavaMx.forget(endpointNumber, handle);
			}
		} catch(MxException e) {
			// when this is not successful, quit anyways
		}
		JavaMx.handles.releaseHandle(handle);		
	}
	
	@Override
	public String toString() {
		return createString(target, Matching.getPort(matchData));
	}

	protected static String createString(MxAddress address, int port) {
		return "OutputStreamImpl:" + address.toString() + "("
				+ Integer.toString(port) + ")";
	}
	
	
	
}
