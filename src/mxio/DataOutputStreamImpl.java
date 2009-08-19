package mxio;

import java.io.IOException;

import mxio.JavaMx.HandleManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataOutputStreamImpl extends DataOutputStream {

	private static final Logger logger = LoggerFactory
	.getLogger(DataOutputStreamImpl.class);

	private class FlushQueue {
		int[] handles;
		MxSendBuffer[] queue;

		int head;
		int elements;

		int size;
		private boolean destroyed = false;

		FlushQueue(int size) {
			head = elements = 0;
			this.size = size;
			queue = new MxSendBuffer[size];
			handles = new int[size];
			for (int i = 0; i < size; i++) {
				handles[i] = JavaMx.handles.getHandle();
			}
		}

		protected void finalize() {
			destroy();
		}

		void flushHead() throws MxException {
			if (elements == 0) {
				return;
			}

			MxSendBuffer buf = queue[head];
			if(buf == null) {
				throw new Error("got null buf from queue");
			}
			queue[head] = null;
                        
                       // long t1 = System.nanoTime();
                        
			int msgSize = -1;

			msgSize = JavaMx.test(endpointNumber, handles[head]);
			int i = 1;
			while(msgSize < 0 && i < Config.SPOLLS) {
				Thread.yield();
				msgSize = JavaMx.test(endpointNumber, handles[head]);
				i++;
			}

                        /*
                        long t2 = System.nanoTime();
                        
                        if (msgSize < 0) {                            
                            System.out.println("No send after " + i + " polls " + (t2-t1));
                        } else { 
                            System.out.println("Message send after " + i + " polls " + (t2-t1));
                        }
                        */
			//msgSize = JavaMx.test(endpointNumber, handles[head], Config.SPOLLS);

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
			MxSendBuffer.recycle(buf);
                      /*  
                        long t3 = System.nanoTime();
                        
                        System.out.println("Message send after " + i + " polls+wait " + (t3-t1));
                      */  
			return;
		}

		boolean doSend(MxSendBuffer buffer, boolean synchronous) throws MxException {
			if(elements == size) {
				return false;
			}

			int tail = (head + elements) % size;
			queue[tail] = buffer;
			elements++;

			if(synchronous) {
				JavaMx.sendSynchronous(buffer.header.buf, buffer.header.capacity(), buffer.payload.buf, buffer.payload.remaining(), endpointNumber, 
						myLink, handles[tail], matchData);
			} else {
				JavaMx.send(buffer.header.buf, buffer.header.capacity(), buffer.payload.buf, buffer.payload.remaining(), endpointNumber, 
					myLink, handles[tail], matchData);
			}

			return true;

		}

		boolean isEmpty() {
			return elements == 0;
		}

		void destroy() {
			if(!destroyed ) {
				for (int i = head; i < head + elements; i++) {
					JavaMx.forget(endpointNumber, handles[i%size]);
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

	private MxAddress target;
	private int endpointNumber;
	private long matchData;
	private int myLink;
	
	private int port;
	
	private static final int syncRate = Config.SYNC_RATE;
	private int sync = 0;
	
	protected DataOutputStreamImpl(MxSocket socket, int endpointNumber, int link,
			long matchData, MxAddress target) {
		super();

		flushQueue = new FlushQueue(Config.FLUSH_QUEUE_SIZE);
		this.endpointNumber = endpointNumber;
		this.matchData = matchData;
		this.target = target;
		this.port = Matching.getPort(matchData);
		myLink = link;
	}

	long doSend(MxSendBuffer buffer) throws IOException {
		buffer.setPort(port);
		long size = buffer.remaining();
		
		boolean sendSync = false;
		sync++;
		if(sync == syncRate) {
			sync = 0;
			sendSync = true;
		}
		
		while(!flushQueue.doSend(buffer, sendSync)) {
			flushHead();
		}
		
		return size;
	}

	void doFlush() throws IOException {
		while(!flushQueue.isEmpty()) {
			flushHead();
		}
	}

	void flushHead() throws MxException {
		flushQueue.flushHead();
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
