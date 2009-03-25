package mxio;

import java.io.EOFException;
import java.io.IOException;
import java.nio.BufferUnderflowException;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class InputStream extends java.io.InputStream {
	
	private static final Logger logger = LoggerFactory
    .getLogger(InputStream.class);
	
	public static final int BUFFER_SIZE = mxio.Config.BUFFER_SIZE;
	
	protected boolean closed = false;
	protected boolean senderClosed = false;

	protected MxSocket socket;
	protected int endpointNumber;
	protected long matchData;

	protected MxAddress source;
	
	private ReceiveBuffer buffer = null;
    
	protected InputStream(MxSocket socket, MxAddress source,
			int endpointNumber, long matchData) throws IOException {
		
		this.socket = socket;
		this.source = source;
		this.endpointNumber = endpointNumber;
		this.matchData = matchData;

		if (logger.isDebugEnabled()) {
			logger.debug("InputStream <-- " + source.toString() + " : " + 
					Integer.toHexString(Matching.getPort(matchData)) 
					+  " created.");
		}
	}
		
	@Override
	public int available() {
		if(buffer == null) {
			return 0;
		}
		return buffer.remaining();
	}

	@Override
	public void close() throws IOException {
		if (logger.isDebugEnabled()) {
			logger.debug("close()");
		}
		if (closed) {
			return;
		} else if(senderClosed) {
			closed = true;
			cleanUp();
		} else {
			closed = true;
			socket.sendCloseMessage(this);
			cleanUp();
		}
	}
	
	protected abstract void cleanUp();

	protected void senderClosedConnection() {
		if (logger.isDebugEnabled()) {
			logger.debug("senderClosedConnection()");
		}
		if(closed || senderClosed) {
			return;
		}
		senderClosed = true;
	}

	protected MxAddress getSource() {
		return source;
	}

	protected int getPort() {
		return Matching.getPort(matchData);
	}
	
	// TODO state the demanded properties of this function
	protected abstract ReceiveBuffer fetchBuffer() throws IOException;
	
	@Override
	public void mark(int arg0) {
		// empty implementation
	}

	@Override
	public boolean markSupported() {
		return false;
	}


	@Override
	public void reset() throws IOException {
		throw new IOException("reset() not supported");
	}
	
    public byte readByte() throws IOException {
        byte result;

        while(!closed) {
	        try {
	            result = buffer.readByte();
	            return result;
	        } catch (BufferUnderflowException e) {
	        	if (logger.isDebugEnabled()) {
		            logger.debug("BufferUnderflowException");
		        }
	        	ReceiveBuffer.recycle(buffer);
    			buffer = null;
	        	buffer = fetchBuffer();
	        } catch (NullPointerException e2) {
	        	if (logger.isDebugEnabled()) {
		            logger.debug("NullPointerException");
		        }
	        	buffer = fetchBuffer();
    			if(buffer == null && senderClosed) {
    				if (logger.isDebugEnabled()) {
    					logger.debug("senderClosed detected in read()");
    				}
    				closed = true;
    				cleanUp();
           	 		return-1;
           	 	}
	        }
        }
        return -1;
    }

    @Override
    public int read() throws IOException {
        try {
            return readByte() & 0377;
        } catch (EOFException e) {
            return -1;
        }
    }
    
    @Override
    public int read(byte[] ref) throws IOException {
        return read(ref, 0, ref.length);
    }

    @Override
    public int read(byte ref[], int off, int len) throws IOException {
    	while(true) {
    		if(closed) {
    			return -1;
    		}

    		if(buffer == null) {
    			buffer = fetchBuffer();
    			if(buffer == null && senderClosed) {
    				if (logger.isDebugEnabled()) {
    					logger.debug("senderClosed detected in read()");
    				}
    				closed = true;
    				cleanUp();
           	 		return-1;
           	 	}
    		} else if(buffer.remaining() == 0) {
    			ReceiveBuffer.recycle(buffer);
    			buffer = null;
    			buffer = fetchBuffer();
    			if(buffer == null && senderClosed) {
    				if (logger.isDebugEnabled()) {
    					logger.debug("senderClosed detected in read()");
    				}
    				closed = true;
    				cleanUp();
           	 		return-1;
           	 	}
    		} else {
    			return buffer.readArray(ref, off, len);
    		}
    	}   	
    }
    
    
    public boolean isSelectableStream() {
    	return false;
    }
    
    public boolean attachedToSelector() {
		return false;
	}
    
    /**
     * @param s The Selector to attach to
     * @return true whenn attached successfully, false when not
     * When a stream is not a selectable stream, it also returns false
     * @throws IOException
     */
    public boolean attach(Selector s) {
		return false;
		
	}

    /** detaches from the selector it is connected to. 
     * Does nothing when the stream is not connected to a selector.
     */
	public void detach() {
		
	}

}
