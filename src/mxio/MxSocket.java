package mxio;

import ibis.util.ThreadPool;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReentrantLock;

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

	private ConcurrentSkipListMap<MxAddress, Integer> links;

	/** LowLatency Streams get even numbers, Selectable streams get odd numbers **/
	private ConcurrentHashMap<Integer, SelectableDataInputStream> selectableDataInputStreams;
	private ConcurrentHashMap<Integer, LowLatencyDataInputStream> lowLatencyDataInputStreams;
	private ConcurrentHashMap<String, DataOutputStreamImpl> dataOutputStreams;

	private int nextsKey = 1;
	private int nextllKey = 2;
	private MxListener listener = null;
	private boolean closed = false, closing = false;
	private int endpointNumber;
	private int sendEndpointNumber;
	private ByteBuffer listenBuf, connectBuf;
	private int listenHandle, connectHandle, ackHandle;
	
	private ReentrantLock ackLock = new ReentrantLock();
	private volatile boolean ackInTransfer = false;

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
		//		sendEndpointNumber = endpointNumber;

		myAddress = new MxAddress(JavaMx.getMyNicId(endpointNumber), JavaMx
				.getMyEndpointId(endpointNumber));

		listenBuf = ByteBuffer.allocateDirect(MAX_CONNECT_MSG_SIZE).order(
				ByteOrder.BIG_ENDIAN);
		listenHandle = JavaMx.handles.getHandle();
		connectBuf = ByteBuffer.allocateDirect(MAX_CONNECT_MSG_SIZE).order(
				ByteOrder.BIG_ENDIAN);
		connectHandle = JavaMx.handles.getHandle();
		ackHandle = JavaMx.handles.getHandle();

		links  = new ConcurrentSkipListMap<MxAddress, Integer>();
		
		selectableDataInputStreams = new ConcurrentHashMap<Integer, SelectableDataInputStream>();
		lowLatencyDataInputStreams = new ConcurrentHashMap<Integer, LowLatencyDataInputStream>();
		dataOutputStreams = new ConcurrentHashMap<String, DataOutputStreamImpl>();

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

		int link = lookup(target);
		if(link == -1) {
			return null;
		}

		// send request
		connectBuf.flip();
		JavaMx.sendSynchronous(connectBuf, connectBuf.position(), connectBuf
				.remaining(), sendEndpointNumber, link, connectHandle,
				Matching.PROTOCOL_CONNECT);
		msgSize = JavaMx.wait(sendEndpointNumber, connectHandle); //TODO timeout
		if (msgSize < 0) {
			throw new MxException("error");
		}

		// read reply
		connectBuf.clear();
		JavaMx.recv(connectBuf, connectBuf.position(), connectBuf.remaining(),
				endpointNumber, connectHandle, Matching.PROTOCOL_CONNECT_REPLY);
		msgSize = JavaMx.wait(endpointNumber, connectHandle); //TODO timeout
		if (msgSize < 0) {
			throw new MxException("error");
		}
		connectBuf.limit(msgSize);
		byte reply = connectBuf.get();
		byte[] replymsg;
		switch (reply) {
		case Connection.ACCEPT:
			long matchData = Matching.construct(Matching.PROTOCOL_DATA,
					connectBuf.getInt());
			DataOutputStreamImpl os = new DataOutputStreamImpl(this, sendEndpointNumber, link,
					matchData, target);
			addDataOutputStream(os);
			replymsg = new byte[connectBuf.remaining()];
			connectBuf.get(replymsg);
			return new Connection(os, reply, replymsg);
		case Connection.REJECT:
			replymsg = new byte[connectBuf.remaining()];
			connectBuf.get(replymsg);
			return new Connection(null, reply, replymsg);
		default:
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

			if (protocol == Matching.PROTOCOL_ACK) {
				if (logger.isDebugEnabled()) {
					logger.debug("ACK message received");
				}
				receiveAck(matching);
			} else if (protocol == Matching.PROTOCOL_DISCONNECT) {
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

	private void receiveAck(long matchData) {
		listenBuf.clear();
		try {
			JavaMx.recv(listenBuf, listenBuf.position(), listenBuf.remaining(),
					endpointNumber, listenHandle, matchData);
			int size = JavaMx.wait(endpointNumber, listenHandle, 1000);
			if (size < 0) {
				if(!JavaMx.cancel(endpointNumber, listenHandle)) {
					size = JavaMx.test(endpointNumber, listenHandle);
				}
			}
		} catch (MxException e) {
			// TODO Auto-generated catch block
			// should not go wrong, the message is already waiting for us
			e.printStackTrace();
			return;
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
		DataOutputStreamImpl os = getDataOutputStream(DataOutputStreamImpl.createString(sender,
				Matching.getPort(matchData)));

		if (os != null) {
			os.receiverClosedConnection();
		}
	}

	private void senderClosedConnection(long matchData) {
		DataInputStream is;
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
			is = lowLatencyDataInputStreams.get(port);	
		} else {
			is = selectableDataInputStreams.get(port);
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
			int link = lookup(source);
			if(link == -1) {
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
			listenBuf.clear();
			return;
		}
	}

	protected DataInputStream accept(ConnectionRequest request) throws MxException {

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

		int link = lookup(request.getSourceAddress());
		if(link == -1) {
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
				getDataInputStream(Matching.getPort(matchData)).close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			JavaMx.cancel(sendEndpointNumber, listenHandle); // TODO
			//error
			request.reject();
			return null;
		}
		return getDataInputStream(Matching.getPort(matchData));
	}

	protected long addConnection(MxAddress source, boolean selectableChannel)
	throws IOException {
		if(selectableChannel) {
			boolean found = false;
			for (long i = 1; i < Integer.MAX_VALUE/2; i++) {
				nextsKey = (nextsKey % Integer.MAX_VALUE) + 2; // 0 will not be used
				if (!selectableDataInputStreams.containsKey(nextsKey)) {
					found = true;
					break;
				}
			}
			if(!found) {
				throw new IOException("maximum number of connections reached");
			}

			long matchData;
			SelectableDataInputStream is;

			try {
				matchData = Matching.construct(Matching.PROTOCOL_DATA, nextsKey);
				is = new SelectableDataInputStream(this, source, endpointNumber(), matchData);

				selectableDataInputStreams.put(nextsKey, is);
				return matchData;
			} catch (IOException e) {
				throw new MxException("Could not create a connection:" + e.getMessage());
			}

		} else {
			boolean found = false;
			for (long i = 0; i < Integer.MAX_VALUE/2; i++) {
				nextllKey = (nextllKey % Integer.MAX_VALUE) + 2; // 0 will not be used
				if (!lowLatencyDataInputStreams.containsKey(nextllKey)) {
					found = true;
					break;
				}
			}
			if(!found) {
				throw new IOException("maximum number of connections reached");
			}

			long matchData;
			LowLatencyDataInputStream is;

			try {
				matchData = Matching.construct(Matching.PROTOCOL_DATA, nextllKey);
				is = new LowLatencyDataInputStream(this, source, endpointNumber(), matchData);

				lowLatencyDataInputStreams.put(nextllKey, is);
				return matchData;
			} catch (IOException e) {
				throw new MxException("Could not create a connection:" + e.getMessage());
			}
		}
	}

	protected DataInputStream getDataInputStream(int port) {
		if(port % 2 == 0) {
			return lowLatencyDataInputStreams.get(port);	
		} else {
			return selectableDataInputStreams.get(port);
		}
	}

	protected SelectableDataInputStream getSelectableDataInputStream(int port) {
		if(port % 2 == 0) {
			return null;	
		} else {
			return selectableDataInputStreams.get(port);
		}
	}

	protected DataInputStream removeDataInputStream(int port) {
		if(port % 2 == 0) {
			return lowLatencyDataInputStreams.remove(port);	
		} else {
			return selectableDataInputStreams.remove(port);
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
		for(Integer link : links.values()) {
			JavaMx.disconnect(link);
			JavaMx.links.releaseLink(link);
		}
	}

	protected int endpointNumber() {
		return endpointNumber;
	}

	protected void addDataOutputStream(DataOutputStreamImpl os) {
		dataOutputStreams.put(os.toString(), os);
	}

	protected DataOutputStreamImpl removeDataOutputStream(String identifier) {
		return dataOutputStreams.remove(identifier);
	}

	protected DataOutputStreamImpl getDataOutputStream(String identifier) {
		return dataOutputStreams.get(identifier);
	}

	// My InputStream closes, notify the sender
	protected void sendCloseMessage(DataInputStream is) {
		// should only be called by the InputStream
		MxAddress target = is.getSource();

		int link = lookup(target);
		if(link == -1) {
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
	}

	protected void sendAck(DataInputStream is) {
		// should only be called by the InputStream
		MxAddress target = is.getSource();

		
		int link = lookup(target);
		if(link == -1) {
			return;
		}

		if(ackLock.tryLock() == false) {
			return;
		}
		try {
			if(ackInTransfer) {
				if(JavaMx.test(sendEndpointNumber, ackHandle, 1000) < 0) {
					JavaMx.forget(sendEndpointNumber, ackHandle);
				}
			}
			JavaMx.send(null, 0, 0, sendEndpointNumber, link, ackHandle,
					Matching.construct(Matching.PROTOCOL_ACK, is.getPort()));
			ackInTransfer = true;
		} catch (MxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			ackLock.unlock();
		}
	}


	public MxAddress getMyAddress() {
		return myAddress;
	}

	int lookup(MxAddress address) {
		Integer link = links.get(address);
		if (link == null) {
			try {
				link = JavaMx.links.getLink();
				if (JavaMx.connect(sendEndpointNumber, link, address.nicId, address.endpointId,
						IBIS_FILTER) == false) {
					// has the other side died??
					JavaMx.links.releaseLink(link);
					return -1;
				}
			} catch (MxException e1) {
				// has the other side died??
				JavaMx.links.releaseLink(link);
				return -1;
			}
			links.put(address, link);
		}
		return link;
	}
}
