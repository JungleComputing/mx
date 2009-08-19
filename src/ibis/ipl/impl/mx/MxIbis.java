
package ibis.ipl.impl.mx;

import ibis.ipl.AlreadyConnectedException;
import ibis.ipl.CapabilitySet;
import ibis.ipl.ConnectionFailedException;
import ibis.ipl.ConnectionRefusedException;
import ibis.ipl.ConnectionTimedOutException;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisCreationFailedException;
import ibis.ipl.IbisStarter;
import ibis.ipl.MessageUpcall;
import ibis.ipl.PortMismatchException;
import ibis.ipl.PortType;
import ibis.ipl.ReceivePortConnectUpcall;
import ibis.ipl.RegistryEventHandler;
import ibis.ipl.SendPortDisconnectUpcall;
import ibis.ipl.impl.IbisIdentifier;
import ibis.ipl.impl.ReceivePort;
import ibis.ipl.impl.SendPort;
import ibis.ipl.impl.SendPortIdentifier;
import ibis.ipl.Credentials;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mxio.Connection;
import mxio.ConnectionRequest;
import mxio.DataOutputStream;
import mxio.MxAddress;
import mxio.MxListener;
import mxio.MxSocket;

public final class MxIbis extends ibis.ipl.impl.Ibis 
implements MxListener {

	static final Logger logger = LoggerFactory
	.getLogger(MxIbis.class);

	private MxSocket socket;

	private MxAddress myAddress;

	private HashMap<ibis.ipl.IbisIdentifier, MxAddress> addresses
	= new HashMap<ibis.ipl.IbisIdentifier, MxAddress>();

	public MxIbis(RegistryEventHandler registryEventHandler,
			IbisCapabilities capabilities, Credentials credentials,
                        byte[] applicationTag, PortType[] types, 
                        Properties userProperties, IbisStarter starter)         
            throws IbisCreationFailedException {
	
            super(registryEventHandler, capabilities, credentials, 
                        applicationTag, types, userProperties, starter);
		this.properties.checkProperties("ibis.ipl.impl.mx.",
				new String[] {"ibis.ipl.impl.mx.mx"}, null, true);
	}

	protected byte[] getData() throws IOException {
		socket = new MxSocket(this);
		myAddress = socket.getMyAddress();

		if (logger.isInfoEnabled()) {
			logger.info("--> MxIbis: address = " + myAddress);
		}

		return myAddress.toBytes();
	}

	/*
	 * // NOTE: this is wrong ? Even though the ibis has left, the
	 * IbisIdentifier may still be floating around in the system... We should
	 * just have some timeout on the cache entries instead...
	 * 
	 * public void left(ibis.ipl.IbisIdentifier id) { super.left(id);
	 * synchronized(addresses) { addresses.remove(id); } }
	 * 
	 * public void died(ibis.ipl.IbisIdentifier id) { super.died(id);
	 * synchronized(addresses) { addresses.remove(id); } }
	 */

	DataOutputStream connect(MxSendPort sp, ibis.ipl.impl.ReceivePortIdentifier rip,
			long timeout, boolean fillTimeout) throws IOException {

		IbisIdentifier id = (IbisIdentifier) rip.ibisIdentifier();
		String name = rip.name();
		MxAddress addr;

		synchronized(addresses) {
			addr = addresses.get(id);
			if (addr == null) {
				addr = MxAddress.fromBytes(id.getImplementationData());
				addresses.put(id, addr);
			}
		}
		
		long startTime = System.currentTimeMillis();

		if (logger.isDebugEnabled()) {
			logger.debug("--> Creating socket for connection to " + name
					+ " at " + addr);
		}

		PortType sendPortType = sp.getPortType();

		do {         
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			java.io.DataOutputStream out = new java.io.DataOutputStream(baos);
			out.writeUTF(name);
			sp.getIdent().writeTo(out);
			sendPortType.writeTo(out);
			out.flush();
			Connection c = socket.connect(addr, baos.toByteArray(), timeout);
			if(c == null) {
				if(timeout > 0) {
					throw new ConnectionTimedOutException("MX Socket timed out", rip);
				} else {
					throw new ConnectionFailedException("MX Socket could not connect to remote socket", rip);
				}
			}
			
			int result = ReceivePort.DENIED;
			DataInputStream in = null;
			byte[] replyMsg = c.getReplyMessage();
			if(replyMsg != null) {
				ByteArrayInputStream bais = new ByteArrayInputStream(c.getReplyMessage());
				in = new DataInputStream(bais);
				result = in.readInt();
			} else { //replyMsg == null
				if (c.getReply() == Connection.ACCEPT) {
					c.getDataOutputStream().close();
					throw new ConnectionFailedException("Invalid connection reply message", rip);
				} else {
					throw new ConnectionRefusedException("connection refused, reply message is missing", rip);
				}				
			}
			DataOutputStream os;

			try {
				switch(result) {
				case ReceivePort.ACCEPTED:
					if(c.getReply() == Connection.ACCEPT) {
						os = c.getDataOutputStream();
					} else {
						//FIXME error
						os = null;
					}
					return os;
				case ReceivePort.ALREADY_CONNECTED:
					throw new AlreadyConnectedException("Already connected", rip);
				case ReceivePort.TYPE_MISMATCH:
					// Read receiveport type from input, to produce a
					// better error message.
					PortType rtp = new PortType(in);
					CapabilitySet s1 = rtp.unmatchedCapabilities(sendPortType);
					CapabilitySet s2 = sendPortType.unmatchedCapabilities(rtp);
					String message = "";
					if (s1.size() != 0) {
						message = message
						+ "\nUnmatched receiveport capabilities: "
						+ s1.toString() + ".";
					}
					if (s2.size() != 0) {
						message = message
						+ "\nUnmatched sendport capabilities: "
						+ s2.toString() + ".";
					}
					throw new PortMismatchException(
							"Cannot connect ports of different port types."
							+ message, rip);
				case ReceivePort.DENIED:
					throw new ConnectionRefusedException(
							"Receiver denied connection", rip);
				case ReceivePort.NO_MANY_TO_X:
					throw new ConnectionRefusedException(
							"Receiver already has a connection and neither ManyToOne nor ManyToMany "
							+ "is set", rip);
				case ReceivePort.NOT_PRESENT:
				case ReceivePort.DISABLED:
					// and try again if we did not reach the timeout...
					
					if (timeout > 0 && System.currentTimeMillis()
							> startTime + timeout) {
						throw new ConnectionTimedOutException(
								"Could not connect", rip);
					}
					try {
							Thread.sleep(timeout);
						} catch (InterruptedException e) {
							// ignore
						}
					break;
				case -1:
					throw new IOException("Encountered EOF in MxIbis.connect");
				default:
					throw new IOException("Illegal opcode in MxIbis.connect");
				}
			} catch(SocketTimeoutException e) {
				throw new ConnectionTimedOutException("Could not connect", rip);
			} finally {
				if (result != ReceivePort.ACCEPTED) {
					try {
						out.close();
					} catch(Throwable e) {
						// ignored
					}
				}
			}
			if(fillTimeout) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// ignore
				}
			}
		} while (true);
	}

	protected synchronized void quit() {
		cleanup();
	}

	public void newConnection(ConnectionRequest request) {
		if (logger.isDebugEnabled()) {
			logger.debug("--> MxIbis got connection request from " + request.getSourceAddress());
		}
		ByteArrayInputStream bais = new ByteArrayInputStream(request.getDescriptor());
		DataInputStream in = new DataInputStream(bais);
		try {
			in.close();
		} catch (IOException e) {
			// ignore
		}

		String name;
		SendPortIdentifier send;
		PortType sp;	
		try {
			name = in.readUTF();
			send = new SendPortIdentifier(in);
			sp = new PortType(in);	
		} catch (IOException e) {
			//TODO something
			e.printStackTrace();
			request.reject();
			return;
		}

		// First, lookup receiveport.
		MxReceivePort rp = (MxReceivePort) findReceivePort(name);

		int result;
		if (rp == null) {
			result = ReceivePort.NOT_PRESENT;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			java.io.DataOutputStream out = new java.io.DataOutputStream(baos);
			try {
				out.writeInt(result);
				out.flush();
			} catch (IOException e) {
				e.printStackTrace();
				throw new Error("error creating connection reply");
			}

			request.setReplyMessage(baos.toByteArray());
			request.reject();
			try {
				out.close();
				in.close();
			} catch (IOException e) {
				// ignore
			}
			if (logger.isDebugEnabled()) {
				logger.debug("newConnectionRequest() finished");
			}
		} else { 
			rp.accept(request, send, sp);
		}
	}



	private void cleanup() {
		try {
			socket.close();
		} catch (Throwable e) {
			// Ignore
		}
	}

	protected SendPort doCreateSendPort(PortType tp, String nm,
			SendPortDisconnectUpcall cU, Properties props) throws IOException {
		return new MxMulticastSendPort(this, tp, nm, cU, props);
		
	}

	protected ReceivePort doCreateReceivePort(PortType tp, String nm,
			MessageUpcall u, ReceivePortConnectUpcall cU, Properties props)
	throws IOException {
		if(tp.hasCapability(PortType.CONNECTION_ONE_TO_ONE) || tp.hasCapability(PortType.CONNECTION_ONE_TO_MANY)) {
			return new MxDefaultReceivePort(this, tp, nm, u, cU, props);
//			return new MxSelectingReceivePort(this, tp, nm, u, cU, props);
		} else {
//			return new MxSelectingReceivePort(this, tp, nm, u, cU, props);
			return new MxDefaultReceivePort(this, tp, nm, u, cU, props);
		}
	}

}
