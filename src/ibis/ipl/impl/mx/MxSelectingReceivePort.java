
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
import mxio.Selector;

/* based on the ReceivePort of TCPIbis */

class MxSelectingReceivePort extends MxReceivePort implements Runnable {

	static final Logger logger = LoggerFactory
    .getLogger(MxSelectingReceivePort.class);
	
	Selector selector;
	
    class ConnectionHandler extends ReceivePortConnectionInfo 
            implements MxProtocol {

        private final InputStream is;
        MxSelectingReceivePort port;

        ConnectionHandler(SendPortIdentifier origin, InputStream is,
        		MxSelectingReceivePort port, BufferedArrayInputStream in)
                throws IOException {
            super(origin, port, in);
            this.port = port;
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

        protected void upcallCalledFinish() {
            super.upcallCalledFinish();
            if (is.attach(selector) == false) {
            	close(new IOException("cannot attach stream to selector"));
        	}
            ThreadPool.createNew(port, "ConnectionHandler");
        }

        /**
         * 
         * @return true when the thread can be reused
         * @throws IOException
         */
        boolean reader() throws IOException {
            byte opcode = -1;

            // Moved here to prevent deadlocks and timeouts when using sun 
            // serialization -- Jason
            if (in == null) { 
                newStream();
            }
            

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
                return true;
            case NEW_RECEIVER:
                if (logger.isDebugEnabled()) {
                    logger.debug(name + ": Got a NEW_RECEIVER from "
                            + origin);
                }
                newStream();
                if (is.attach(selector) == false) {	
	        		throw new IOException("cannot attach stream to selector");
	        	}
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
                if (m.finishCalledInUpcall()) {
                    return false;
                }
                if (is.attach(selector) == false) {	
	        		throw new IOException("cannot attach stream to selector");
	        	}
                break;
            default:
            	if (is.attach(selector) == false) {	
	        		throw new IOException("cannot attach stream to selector");
	        	}
                throw new IOException(name + ": Got illegal opcode "
                        + opcode + " from " + origin);
            }
            return true;
        }
    }

    MxSelectingReceivePort(Ibis ibis, PortType type, String name, MessageUpcall upcall,
            ReceivePortConnectUpcall connUpcall, Properties props) throws IOException {
        super(ibis, type, name, upcall, connUpcall, props);
        selector = new Selector();
        ThreadPool.createNew(this, "ConnectionHandler");
    }

    public void messageArrived(ReadMessage msg) {
        super.messageArrived(msg);
        if (upcall == null) {
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
        return super.getMessage(timeout);
    }

    public synchronized void closePort(long timeout) {
        ReceivePortConnectionInfo conns[] = connections();
        if (upcall == null && conns.length > 0) {
            ThreadPool.createNew(this,
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
    		InputStream is = req.accept(true);
    		if(is == null) {
    			result = DENIED;
    			this.lostConnection(origin, new IOException("ChannelManager denied connection"));
    		} else {
	            synchronized(this) {
	                try {
	                	@SuppressWarnings("unused")
						ConnectionHandler conn = new ConnectionHandler(origin, is, this, new BufferedArrayInputStream(is, BUFSIZE));
	    	        	//FIXME do not attach when no selector is used?
	                	if (is.attach(selector) == false) {	
	    	        		throw new IOException("cannot attach stream to selector");
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

	public void run() {
		long timeout = 1000;
		while(!closed || connections().length > 0) {
			if(!NextIOAction(timeout)) {
				return;
			}
		}
	}

	/**
	 * 
	 * @param timeout
	 * @return true when thread can be reused
	 */
	private boolean NextIOAction(long timeout) {
		// TODO Auto-generated method stub
		if (logger.isDebugEnabled()) {
            logger.debug("NextIOAction");
        }
		InputStream is = selector.select(timeout);
		if(is == null) {
        	if (logger.isDebugEnabled()) {
                logger.debug("No stream selected");
            }
			return true;
		}
		ConnectionHandler ch = findHandler(is);
		if(ch == null) {
        	if (logger.isDebugEnabled()) {
                logger.debug("Trying to read from a stream that is not mine");
            }
			return true;
		}
		try {
			return ch.reader();
		} catch (IOException e) {
			// TODO we ignore it for now
		}
		return true;
	}

	private ConnectionHandler findHandler(InputStream is) {
		ConnectionHandler[] connections =  connections();
		for(int i = 0; i < connections.length; i++) {
			if(connections[i].is == is) {
				return connections[i];
			}
		}
		return null;
	}
	
	@Override
    public synchronized ConnectionHandler[] connections() {
        return connections.values().toArray(new ConnectionHandler[0]);
    }
}
