package mxio;

import ibis.util.ThreadPool;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MxSocket implements Runnable {

	private static final Logger logger = LoggerFactory
    .getLogger(MxSocket.class);

	private static final int IBIS_FILTER = 0xdada1313;
	private static final long POLL_FOR_CLOSE_INTERVAL = 500;
	
	protected static final int MAX_CONNECT_MSG_SIZE = 2048 + MxAddress.SIZE;
	// TODO limit on CONNECT message size, document this
	
	protected static final int MAX_CONNECT_REPLY_MSG_SIZE = MAX_CONNECT_MSG_SIZE - 5;
	// listenBuf - int - byte;

	public static boolean available() {
		return JavaMx.initialized;
	}

	private MxAddress myAddress;
	
	/** LowLatency Streams get even numbers, Selectable streams get odd numbers **/
	private ConcurrentHashMap<Integer, SelectableInputStream> selectableInputStreams;
	private ConcurrentHashMap<Integer, LowLatencyInputStream> lowLatencyInputStreams;
	private ConcurrentHashMap<String, OutputStreamImpl> outputStreams;
	
	private int nextsKey = 1;
	private int nextllKey = 2;
	private MxListener listener = null;
	private boolean closed = false, closing = false;
	private int endpointNumber;
	private int sendEndpointNumber;
	private ByteBuffer listenBuf, connectBuf;
	private int listenHandle, connectHandle;

	DeliveryThread deliveryThread = null;
	
	public MxSocket(MxListener listener) throws MxException {
		if (listener == null) {
			throw new MxException("no listener");
		}
		this.listener = listener;
		
		if (JavaMx.initialized == false) {
			throw new MxException("MxSocket: could not initialize JavaMX");
		}
		endpointNumber = JavaMx.newEndpoint(IBIS_FILTER);
		sendEndpointNumber = JavaMx.newEndpoint(IBIS_FILTER);

		myAddress = new MxAddress(JavaMx.getMyNicId(endpointNumber), JavaMx
				.getMyEndpointId(endpointNumber));

		listenBuf = ByteBuffer.allocateDirect(MAX_CONNECT_MSG_SIZE).order(
				ByteOrder.BIG_ENDIAN);
		listenHandle = JavaMx.handles.getHandle();
		connectBuf = ByteBuffer.allocateDirect(MAX_CONNECT_MSG_SIZE).order(
				ByteOrder.BIG_ENDIAN);
		connectHandle = JavaMx.handles.getHandle();

		selectableInputStreams = new ConcurrentHashMap<Integer, SelectableInputStream>();
		lowLatencyInputStreams = new ConcurrentHashMap<Integer, LowLatencyInputStream>();
		outputStreams = new ConcurrentHashMap<String, OutputStreamImpl>();

		ThreadPool.createNew(this, "MxSocket " + endpointNumber + " - "
				+ sendEndpointNumber);


	}
	
	public Connection connect(MxAddress target, byte[] descriptor, long timeout) 
		throws MxException {
		// TODO catch exceptions, forward them
		int msgSize;

		connectBuf.clear();
		try {
			connectBuf.put(myAddress.toBytes());
			connectBuf.put(descriptor);
		} catch (BufferOverflowException e) {
			throw new MxException("descriptor too long.");
		}

		int link = JavaMx.links.getLink();
		try {
			if (JavaMx.connect(sendEndpointNumber, link, target.nicId, target.endpointId,
					IBIS_FILTER, timeout) == false) {
				// timeout
				JavaMx.links.releaseLink(link);
				return null;
			}
		} catch (MxException e) {
			JavaMx.links.releaseLink(link);
			throw new MxException("error: " + e.getMessage());
		}

		// send request
		connectBuf.flip();
		JavaMx.sendSynchronous(connectBuf, connectBuf.position(), connectBuf
				.remaining(), sendEndpointNumber, link, connectHandle,
				Matching.PROTOCOL_CONNECT);
		msgSize = JavaMx.wait(sendEndpointNumber, connectHandle); //TODO timeout
		if (msgSize < 0) {
			// FIXME error
			JavaMx.disconnect(link);
			JavaMx.links.releaseLink(link);
			throw new MxException("error");
		}
		
		// read reply
		connectBuf.clear();
		JavaMx.recv(connectBuf, connectBuf.position(), connectBuf.remaining(),
				endpointNumber, connectHandle, Matching.PROTOCOL_CONNECT_REPLY);
		msgSize = JavaMx.wait(endpointNumber, connectHandle); //TODO timeout
		if (msgSize < 0) {
			// FIXME error
			JavaMx.disconnect(link);
			JavaMx.links.releaseLink(link);
			throw new MxException("error");
		}
		connectBuf.limit(msgSize);
		byte reply = connectBuf.get();
		byte[] replymsg;
		switch (reply) {
		case Connection.ACCEPT:
			long matchData = Matching.construct(Matching.PROTOCOL_DATA,
					connectBuf.getInt());
			OutputStreamImpl os = new OutputStreamImpl(this, sendEndpointNumber, link,
					matchData, target);
			addOutputStream(os);
			replymsg = new byte[connectBuf.remaining()];
			connectBuf.get(replymsg);
			return new Connection(os, reply, replymsg);
		case Connection.REJECT:
			// TODO clean up
			JavaMx.disconnect(link);
			JavaMx.links.releaseLink(link);
			replymsg = new byte[connectBuf.remaining()];
			connectBuf.get(replymsg);
			return new Connection(null, reply, replymsg);
		default:
			JavaMx.disconnect(link);
			JavaMx.links.releaseLink(link);
			throw new MxException("invalid reply");
		}
	}

	public void run() {
		// TODO create a thread for listening and control messages

		while (!closed) {
			long matching = JavaMx.waitForMessage(endpointNumber,
					POLL_FOR_CLOSE_INTERVAL, Matching.ENDPOINT_TRAFFIC,
					Matching.ENDPOINT_THREAD_TRAFFIC_MASK);

			if (matching == Matching.NONE) {
				// no message arrived, timeout
				continue;
			}
			long protocol = Matching.getProtocol(matching);

			if (protocol == Matching.PROTOCOL_DISCONNECT) {
				if (logger.isDebugEnabled()) {
					logger.debug("DISCONNECT message received");
				}
				// remote SendPort closes
				senderClosedConnection(matching);
			} else if (protocol == Matching.PROTOCOL_CLOSE) {
				if (logger.isDebugEnabled()) {
					logger.debug("CLOSE message received");
				}
				// remote ReceivePort closes
				receiverClosedConnection(matching);
			} else if (protocol == Matching.PROTOCOL_CONNECT) {
				if (logger.isDebugEnabled()) {
					logger.debug("CONNECT message received");
				}
				if (closing) {
					// TODO reject the request and continue
				}
				try {
					listen(matching);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				if (logger.isDebugEnabled()) {
					logger.debug("Unknown message received");
				}
				// unknown control message arrived
				/* FIXME read it to prevent a potential deadlock by buffer space
				 * shortage at the MX device when many of such message arrive?
				 */  
			}
		}
		if (logger.isDebugEnabled()) {
			logger.debug("MxSocket closed");
		}
	}

	private void receiverClosedConnection(long matchData) {
		listenBuf.clear();
		try {
			JavaMx.recv(listenBuf, listenBuf.position(), listenBuf.remaining(),
					endpointNumber, listenHandle, matchData);
			int size = JavaMx.wait(endpointNumber, listenHandle);
			if (size < 0) {
				return; // error
			}
			listenBuf.limit(listenBuf.position() + size);
		} catch (MxException e) {
			// TODO Auto-generated catch block
			// should not go wrong, the message is already waiting for us
			e.printStackTrace();
			return;
		}
		MxAddress sender = MxAddress.fromByteBuffer(listenBuf);
		OutputStreamImpl os = getOutputStream(OutputStreamImpl.createString(sender,
				Matching.getPort(matchData)));

		if (os != null) {
			os.receiverClosedConnection();
		}
	}

	private void senderClosedConnection(long matchData) {
		InputStream is;
		try {
			JavaMx.recv(null, 0, 0, endpointNumber, listenHandle, matchData);
			JavaMx.wait(endpointNumber, listenHandle);
		} catch (MxException e) {
			// TODO Auto-generated catch block
			// should not go wrong, the message is already waiting for us
			e.printStackTrace();
		}
		int port = Matching.getPort(matchData);
		if(port %2 == 0) {
			is = lowLatencyInputStreams.get(port);	
		} else {
			is = selectableInputStreams.get(port);
		}
		
		if (is == null) {
			return; // bogus message, discard it
		}
		is.senderClosedConnection();
	}

	
	
	private void listen(long matchData) throws IOException {
		// TODO catch exceptions
		ConnectionRequest request = null;

		listenBuf.clear();
		int msgSize = 0;
		try {
			JavaMx.recv(listenBuf, listenBuf.position(), listenBuf.remaining(),
					endpointNumber, listenHandle, matchData);
			msgSize = JavaMx.wait(endpointNumber, listenHandle);
		} catch (MxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new MxException("error");
		}
		if (msgSize < 0) {
			// TODO error
			throw new MxException("error");
		}
		listenBuf.limit(msgSize);
		MxAddress source = MxAddress.fromByteBuffer(listenBuf);
		if (source == null) {
			return;
		}
		byte[] descriptor = new byte[listenBuf.remaining()];
		listenBuf.get(descriptor);
		request = new ConnectionRequest(this, source, descriptor);

		listener.newConnection(request);
		// TODO read request
		switch (request.status) {
		case ConnectionRequest.ACCEPTED:
			// accept() already sent the reply message
			return;
		case ConnectionRequest.PENDING:
			// user did not accept it, so we reject it
			request.reject();
		case ConnectionRequest.REJECTED:
			int link = JavaMx.links.getLink();
			if (!JavaMx.connect(sendEndpointNumber, link, source.nicId,
					source.endpointId, IBIS_FILTER, 1000)) {
				// error, stop
				JavaMx.links.releaseLink(link);
				return;
			}
			listenBuf.clear();
			listenBuf.put(Connection.REJECT);
			listenBuf.put(request.replyMessage, 0, request.msgSize);
			listenBuf.flip();
			JavaMx.send(listenBuf, listenBuf.position(), listenBuf
					.remaining(), sendEndpointNumber, link, listenHandle,
					Matching.PROTOCOL_CONNECT_REPLY);
			msgSize = JavaMx.wait(sendEndpointNumber, listenHandle, 1000);
			if (msgSize < 0) {
				// timeout
				JavaMx.cancel(sendEndpointNumber, listenHandle); // TODO
			}
			JavaMx.links.releaseLink(link);
			listenBuf.clear();
			return;
		}
	}

	protected InputStream accept(ConnectionRequest request) throws MxException {
		
		long matchData;
		try {
			boolean selectable = request.selectable();
			matchData = addConnection(request.getSourceAddress(), selectable);
			if(selectable && deliveryThread == null) {
				deliveryThread = new DeliveryThread(this, Config.DELIVERY_THREAD_BUFFERS);
				ThreadPool.createNew(this.deliveryThread, "MxSocket " + endpointNumber + " - "
						+ sendEndpointNumber + " deliveryThread");
			}
		} catch (IOException e) {
			// TODO handle this
			request.reject();
			return null;
		}
		
		// TODO check listenBuf
		int link = JavaMx.links.getLink();
		if (!JavaMx.connect(sendEndpointNumber, link, request.getSourceAddress().nicId,
				request.getSourceAddress().endpointId, IBIS_FILTER, 1000)) {
			// error, stop
			JavaMx.links.releaseLink(link);
			// TODO EXCEPTION
			request.reject();
			return null;
		}

		listenBuf.clear();
		listenBuf.put(Connection.ACCEPT);
		listenBuf.putInt(Matching.getPort(matchData));
		listenBuf.put(request.replyMessage, 0, request.msgSize);
		listenBuf.flip();
		JavaMx.send(listenBuf, listenBuf.position(), listenBuf
				.remaining(), sendEndpointNumber, link, listenHandle,
				Matching.PROTOCOL_CONNECT_REPLY);
		int msgSize = -1;
		try {
			msgSize = JavaMx.wait(sendEndpointNumber, listenHandle, 1000);
		} catch (MxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (msgSize < 0) {
			// TODO timeout
			try {
				getInputStream(Matching.getPort(matchData)).close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			JavaMx.cancel(sendEndpointNumber, listenHandle); // TODO
			JavaMx.links.releaseLink(link);
			//error
			request.reject();
			return null;
		}
		JavaMx.links.releaseLink(link);	
		return getInputStream(Matching.getPort(matchData));
	}

	protected long addConnection(MxAddress source, boolean selectableChannel)
		throws IOException {
		if(selectableChannel) {
			boolean found = false;
			for (long i = 1; i < Integer.MAX_VALUE/2; i++) {
				nextsKey = (nextsKey % Integer.MAX_VALUE) + 2; // 0 will not be used
				if (!selectableInputStreams.containsKey(nextsKey)) {
					found = true;
					break;
				}
			}
			if(!found) {
				throw new IOException("maximum number of connections reached");
			}
			
			long matchData;
			SelectableInputStream is;
			
			try {
				matchData = Matching.construct(Matching.PROTOCOL_DATA, nextsKey);
				is = new SelectableInputStream(this, source, endpointNumber(), matchData);

				selectableInputStreams.put(nextsKey, is);
				return matchData;
			} catch (IOException e) {
				throw new MxException("Could not create a connection:" + e.getMessage());
			}
			
		} else {
			boolean found = false;
			for (long i = 0; i < Integer.MAX_VALUE/2; i++) {
				nextllKey = (nextllKey % Integer.MAX_VALUE) + 2; // 0 will not be used
				if (!lowLatencyInputStreams.containsKey(nextllKey)) {
					found = true;
					break;
				}
			}
			if(!found) {
				throw new IOException("maximum number of connections reached");
			}
			
			long matchData;
			LowLatencyInputStream is;
			
			try {
				matchData = Matching.construct(Matching.PROTOCOL_DATA, nextllKey);
				is = new LowLatencyInputStream(this, source, endpointNumber(), matchData);
				
				lowLatencyInputStreams.put(nextllKey, is);
				return matchData;
			} catch (IOException e) {
				throw new MxException("Could not create a connection:" + e.getMessage());
			}
		}
	}
	
	protected InputStream getInputStream(int port) {
		if(port % 2 == 0) {
			return lowLatencyInputStreams.get(port);	
		} else {
			return selectableInputStreams.get(port);
		}
	}
	
	protected SelectableInputStream getSelectableInputStream(int port) {
		if(port % 2 == 0) {
			return null;	
		} else {
			return selectableInputStreams.get(port);
		}
	}
	
	protected InputStream removeInputStream(int port) {
		if(port % 2 == 0) {
			return lowLatencyInputStreams.remove(port);	
		} else {
			return selectableInputStreams.remove(port);
		}
	}
	
	public void close() {
		closing = true;
		if(deliveryThread != null) {
			deliveryThread.close();
		}
		
		// TODO check for channelManagers and listen thread to finish??
		JavaMx.handles.releaseHandle(listenHandle);
		JavaMx.handles.releaseHandle(connectHandle);
		closed = true;
	}

	protected int endpointNumber() {
		return endpointNumber;
	}

	protected void addOutputStream(OutputStreamImpl os) {
		outputStreams.put(os.toString(), os);
	}

	protected OutputStreamImpl removeOutputStream(String identifier) {
		return outputStreams.remove(identifier);
	}

	protected OutputStreamImpl getOutputStream(String identifier) {
		return outputStreams.get(identifier);
	}

	// My InputStream closes, notify the sender
	protected void sendCloseMessage(InputStream is) {
		// should only be called by the InputStream
		MxAddress target = is.getSource();

		int link = JavaMx.links.getLink();
		try {
			if (JavaMx.connect(sendEndpointNumber, link, target.nicId, target.endpointId,
					IBIS_FILTER) == false) {
				// has the other side died??
				JavaMx.links.releaseLink(link);
				return;
			}
		} catch (MxException e1) {
			// has the other side died??
			JavaMx.links.releaseLink(link);
			return;
		}

		connectBuf.clear();
		connectBuf.put(myAddress.toBytes());
		connectBuf.flip();
		try {
			JavaMx.send(connectBuf, connectBuf.position(),
					connectBuf.remaining(), sendEndpointNumber, link, connectHandle,
					Matching.construct(Matching.PROTOCOL_CLOSE, is.getPort()));

			JavaMx.wait(sendEndpointNumber, connectHandle, 1000);
		} catch (MxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		JavaMx.disconnect(link);
		JavaMx.links.releaseLink(link);

	}

	public MxAddress getMyAddress() {
		return myAddress;
	}
}
