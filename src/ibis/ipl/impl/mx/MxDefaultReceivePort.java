
package ibis.ipl.impl.mx;

import ibis.io.BufferedArrayInputStream;
import ibis.ipl.MessageUpcall;
import ibis.ipl.PortType;
import ibis.ipl.ReceivePortConnectUpcall;
import ibis.ipl.impl.Ibis;
import ibis.ipl.impl.ReadMessage;
import ibis.ipl.impl.ReceivePort;
import ibis.ipl.impl.ReceivePortConnectionInfo;
import ibis.ipl.impl.SendPortIdentifier;
import ibis.util.ThreadPool;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mxio.ConnectionRequest;
import mxio.InputStream;

/* based on the ReceivePort of TCPIbis */

class MxDefaultReceivePort extends MxReceivePort {

	static final Logger logger = LoggerFactory
    .getLogger(MxDefaultReceivePort.class);
	
    class ConnectionHandler extends ReceivePortConnectionInfo 
            implements Runnable, MxProtocol {

        private final InputStream is;

        ConnectionHandler(SendPortIdentifier origin, InputStream is,
                ReceivePort port, BufferedArrayInputStream in)
                throws IOException {
            super(origin, port, in);
            this.is = is;
        }

        public void close(Throwable e) {
            super.close(e);
            try {
                is.close();
            } catch (Throwable x) {
                // ignore
            }
        }

        public void run() {
            try {
                reader(false);
            } catch (Throwable e) {
                logger.debug("ConnectionHandler.run, connected "
                        + "to " + origin + ", caught exception", e);
                close(e);
            }
        }

        protected void upcallCalledFinish() {
            super.upcallCalledFinish();
            ThreadPool.createNew(this, "ConnectionHandler");
        }

        void reader(boolean noThread) throws IOException {
            byte opcode = -1;

            // Moved here to prevent deadlocks and timeouts when using sun 
            // serialization -- Jason
            if (in == null) { 
                newStream();
            }
            
            while (in != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug(name + ": handler for " + origin + " woke up");
                }
                try {
                	opcode = in.readByte();
                } catch (EOFException e ){
                	opcode = -1;
                }
                switch (opcode) {
                case -1:
                	// in closed
                    if (logger.isDebugEnabled()) {
                        logger.debug(name
                                + ": inputstream closed from "
                                + origin);
                    }
                    close(null);
                    return;
                case NEW_RECEIVER:
                    if (logger.isDebugEnabled()) {
                        logger.debug(name + ": Got a NEW_RECEIVER from "
                                + origin);
                    }
                    newStream();
                    break;
                case NEW_MESSAGE:
                    if (logger.isDebugEnabled()) {
                        logger.debug(name + ": Got a NEW_MESSAGE from "
                                + origin);
                    }
                    message.setFinished(false);
                    if (numbered) {
                        message.setSequenceNumber(message.readLong());
                    }
                    ReadMessage m = message;
                    messageArrived(m);
                    // Note: if upcall calls finish, a new message is
                    // allocated, so we cannot look at "message" anymore.
                    if (noThread || m.finishCalledInUpcall()) {
                        return;
                    }
                    break;
                default:
                    throw new IOException(name + ": Got illegal opcode "
                            + opcode + " from " + origin);
                }
            }
        }
    }
    
    private final boolean no_connectionhandler_thread;

    private boolean reader_busy = false;

    MxDefaultReceivePort(Ibis ibis, PortType type, String name, MessageUpcall upcall,
            ReceivePortConnectUpcall connUpcall, Properties props) throws IOException {
        super(ibis, type, name, upcall, connUpcall, props);

        no_connectionhandler_thread = upcall == null && connUpcall == null
                && type.hasCapability(PortType.CONNECTION_ONE_TO_ONE)
                && !type.hasCapability(PortType.RECEIVE_POLL)
                && !type.hasCapability(PortType.RECEIVE_TIMEOUT);
        
    }

    public void messageArrived(ReadMessage msg) {
        super.messageArrived(msg);
        if (! no_connectionhandler_thread && upcall == null) {
            synchronized(this) {
                // Wait until the message is finished before starting to
                // read from the stream again ...
                while (! msg.isFinished()) {
                    try {
                        wait();
                    } catch(Exception e) {
                        // Ignored
                    }
                }
            }
        }
    }

    public ReadMessage getMessage(long timeout) throws IOException {
        if (no_connectionhandler_thread) {
            // Allow only one reader in.
            synchronized(this) {
                while (reader_busy && ! closed) {
                    try {
                        wait();
                    } catch(Exception e) {
                        // ignored
                    }
                }
                if (closed) {
                    throw new IOException("receive() on closed port");
                }
                reader_busy = true;
            }
            // Since we don't have any threads or timeout here, this 'reader' 
            // call directly handles the receive.              
            for (;;) {
                // Wait until there is a connection            
                synchronized(this) {
                    while (connections.size() == 0 && ! closed) {
                        try {
                            wait();
                        } catch (Exception e) {
                            /* ignore */
                        }
                    }

                    // Wait until the current message is done
                    while (message != null && ! closed) {
                        try {
                            wait();
                        } catch (Exception e) {
                            /* ignore */
                        }
                    }
                    if (closed) {
                        reader_busy = false;
                        notifyAll();
                        throw new IOException("receive() on closed port");
                    }
                }

                ReceivePortConnectionInfo conns[] = connections();
                // Note: This call does NOT always result in a message!
                ((ConnectionHandler)conns[0]).reader(true);
                synchronized(this) {
                    if (message != null) {
                        reader_busy = false;
                        notifyAll();
                        return message;
                    }
                }
            }
        } else {
            return super.getMessage(timeout);
        }
    }

    public synchronized void closePort(long timeout) {
        ReceivePortConnectionInfo conns[] = connections();
        if (no_connectionhandler_thread && conns.length > 0) {
            ThreadPool.createNew((ConnectionHandler) conns[0],
                    "ConnectionHandler");
        }
        super.closePort(timeout);
    } 
    
    void accept(ConnectionRequest req, SendPortIdentifier origin, PortType sp) {
    	int result = connectionAllowed(origin, sp);
    	
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);        
        try {
			out.writeInt(result);
	        if (result == ReceivePort.TYPE_MISMATCH) {
	            getPortType().writeTo(out);
	        }
	        out.flush();
        } catch (IOException e) {
			e.printStackTrace();
			throw new Error("error creating connection reply");
		}
        req.setReplyMessage(baos.toByteArray());    	
    	
    	if (result == ACCEPTED) {
    		//TODO selectable
    		InputStream is = req.accept(false);
    		if(is == null) {
    			result = DENIED;
    			this.lostConnection(origin, new IOException("ChannelManager denied connection"));
    		} else {
	            synchronized(this) {
	                try {
	                	//TODO
	                	ConnectionHandler conn = new ConnectionHandler(origin, is, this, new BufferedArrayInputStream(is, BUFSIZE));
	                	if (! no_connectionhandler_thread) {
	                    	ThreadPool.createNew(conn, "ConnectionHandler");
	                    }
					} catch (IOException e) {
						result = DENIED;
						if (logger.isDebugEnabled()) {
        	                logger.debug("DENIED: " + e.getMessage());
        	            }
						try {
							is.close();
						} catch (IOException e1) {
							// ignore
						}
						
						
						//TODO set error message
						this.lostConnection(origin, e);
					}
	            }
	        	
	        	if (logger.isDebugEnabled()) {
	                logger.debug("--> S RP = " + name + ": "
	                        + ReceivePort.getString(result));
	            }
    		}
    	} else {  		
            req.reject();
    	}
    }
}
