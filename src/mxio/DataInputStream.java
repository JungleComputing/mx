package mxio;

import java.io.EOFException;
import java.io.IOException;
import java.nio.BufferUnderflowException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DataInputStream extends ibis.io.DataInputStream {
	
	private static final Logger logger = LoggerFactory
    .getLogger(DataInputStream.class);
	
	public static final int BUFFER_SIZE = mxio.Config.BUFFER_SIZE;
	
	protected boolean closed = false;
	protected boolean senderClosed = false;

	protected MxSocket socket;
	protected int endpointNumber;
	protected long matchData;

	protected MxAddress source;
	
	private MxReceiveBuffer buffer = null;
	private long bytesRead = 0;
    
	protected DataInputStream(MxSocket socket, MxAddress source,
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
	protected abstract MxReceiveBuffer fetchBuffer() throws IOException;
	
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
    	while(!closed) {
        	try {
	        	while(buffer.remaining() == 0) {
	        		MxReceiveBuffer.recycle(buffer);
	    			buffer = null;
		        	buffer = fetchBuffer();
	        	}
	        	byte result = buffer.readByte();
	        	bytesRead++;
	        	return result;
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
    				throw new EOFException();
           	 	}
	        }
        }
    	throw new EOFException();
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
    			MxReceiveBuffer.recycle(buffer);
    			buffer = null;
    			buffer = fetchBuffer();
    			if(buffer == null && senderClosed) {
    				if (logger.isDebugEnabled()) {
    					logger.debug("senderClosed detected in read()");
    				}
    				closed = true;
    				cleanUp();
           	 		return -1;
           	 	}
    		} else {
    			int result = buffer.readArray(ref, off, len);
    			bytesRead += result;
    			return result;
    		}
    	}   	
    }
       
    @Override
	public int bufferSize() {
    	return Config.REPORTED_BUFFER_SIZE;
	}

	@Override
	public long bytesRead() {
		return bytesRead;
	}

	@Override
	public boolean readBoolean() throws IOException {
		return readByte() != 0;
	}

	@Override
	public void resetBytesRead() {
		bytesRead = 0;
	}

	@Override
	public void readArray(boolean[] destination, int offset, int length)
			throws IOException {
		if(closed) {
			throw new EOFException();
		}
		int last = offset + length;
		for(int i = offset; i < last; i++) {
			destination[i] = readBoolean();
		}
	}

	@Override
	public void readArray(byte[] destination, int offset, int length) throws IOException {
		int elementsRead = 0;
		while(elementsRead < length) {
    		if(closed) {
    			throw new EOFException();
    		}

    		if(buffer == null) {
    			buffer = fetchBuffer();
    			if(buffer == null && senderClosed) {
    				if (logger.isDebugEnabled()) {
    					logger.debug("senderClosed detected in read()");
    				}
    				closed = true;
    				cleanUp();
    				throw new EOFException();
           	 	}
    		} else if(buffer.remaining() == 0) {
    			MxReceiveBuffer.recycle(buffer);
    			buffer = null;
    			buffer = fetchBuffer();
    			if(buffer == null && senderClosed) {
    				if (logger.isDebugEnabled()) {
    					logger.debug("senderClosed detected in read()");
    				}
    				closed = true;
    				cleanUp();
    				throw new EOFException();
           	 	}
    		} else {
    			int r = buffer.readArray(destination, offset + elementsRead, length - elementsRead);
    			elementsRead += r;
    			bytesRead += r * SizeOf.BYTE;
    		}
    	}		
	}

	@Override
	public void readArray(char[] destination, int offset, int length) throws IOException {
		int elementsRead = 0;
		while(elementsRead < length) {
    		if(closed) {
    			throw new EOFException();
    		}

    		if(buffer == null) {
    			buffer = fetchBuffer();
    			if(buffer == null && senderClosed) {
    				if (logger.isDebugEnabled()) {
    					logger.debug("senderClosed detected in read()");
    				}
    				closed = true;
    				cleanUp();
    				throw new EOFException();
           	 	}
    		} else if(buffer.remaining() == 0) {
    			MxReceiveBuffer.recycle(buffer);
    			buffer = null;
    			buffer = fetchBuffer();
    			if(buffer == null && senderClosed) {
    				if (logger.isDebugEnabled()) {
    					logger.debug("senderClosed detected in read()");
    				}
    				closed = true;
    				cleanUp();
    				throw new EOFException();
           	 	}
    		} else {
    			int r = buffer.readArray(destination, offset + elementsRead, length - elementsRead);
    			elementsRead += r;
    			bytesRead += r * SizeOf.CHAR;
    		}
    	}		
	}

	@Override
	public void readArray(double[] destination, int offset, int length) throws IOException {
		int elementsRead = 0;
		while(elementsRead < length) {
    		if(closed) {
    			throw new EOFException();
    		}

    		if(buffer == null) {
    			buffer = fetchBuffer();
    			if(buffer == null && senderClosed) {
    				if (logger.isDebugEnabled()) {
    					logger.debug("senderClosed detected in read()");
    				}
    				closed = true;
    				cleanUp();
    				throw new EOFException();
           	 	}
    		} else if(buffer.remaining() == 0) {
    			MxReceiveBuffer.recycle(buffer);
    			buffer = null;
    			buffer = fetchBuffer();
    			if(buffer == null && senderClosed) {
    				if (logger.isDebugEnabled()) {
    					logger.debug("senderClosed detected in read()");
    				}
    				closed = true;
    				cleanUp();
    				throw new EOFException();
           	 	}
    		} else {
    			int r = buffer.readArray(destination, offset + elementsRead, length - elementsRead);
    			elementsRead += r;
    			bytesRead += r * SizeOf.DOUBLE;
    		}
    	}		
	}

	@Override
	public void readArray(float[] destination, int offset, int length) throws IOException {
		int elementsRead = 0;
		while(elementsRead < length) {
    		if(closed) {
    			throw new EOFException();
    		}

    		if(buffer == null) {
    			buffer = fetchBuffer();
    			if(buffer == null && senderClosed) {
    				if (logger.isDebugEnabled()) {
    					logger.debug("senderClosed detected in read()");
    				}
    				closed = true;
    				cleanUp();
    				throw new EOFException();
           	 	}
    		} else if(buffer.remaining() == 0) {
    			MxReceiveBuffer.recycle(buffer);
    			buffer = null;
    			buffer = fetchBuffer();
    			if(buffer == null && senderClosed) {
    				if (logger.isDebugEnabled()) {
    					logger.debug("senderClosed detected in read()");
    				}
    				closed = true;
    				cleanUp();
    				throw new EOFException();
           	 	}
    		} else {
    			int r = buffer.readArray(destination, offset + elementsRead, length - elementsRead);
    			elementsRead += r;
    			bytesRead += r * SizeOf.FLOAT;
    		}
    	}			
	}

	@Override
	public void readArray(int[] destination, int offset, int length) throws IOException {
		int elementsRead = 0;
		while(elementsRead < length) {
    		if(closed) {
    			throw new EOFException();
    		}

    		if(buffer == null) {
    			buffer = fetchBuffer();
    			if(buffer == null && senderClosed) {
    				if (logger.isDebugEnabled()) {
    					logger.debug("senderClosed detected in read()");
    				}
    				closed = true;
    				cleanUp();
    				throw new EOFException();
           	 	}
    		} else if(buffer.remaining() == 0) {
    			MxReceiveBuffer.recycle(buffer);
    			buffer = null;
    			buffer = fetchBuffer();
    			if(buffer == null && senderClosed) {
    				if (logger.isDebugEnabled()) {
    					logger.debug("senderClosed detected in read()");
    				}
    				closed = true;
    				cleanUp();
    				throw new EOFException();
           	 	}
    		} else {
    			int r = buffer.readArray(destination, offset + elementsRead, length - elementsRead);
    			elementsRead += r;
    			bytesRead += r * SizeOf.INT;
    		}
    	}		
	}

	@Override
	public void readArray(long[] destination, int offset, int length) throws IOException {
		int elementsRead = 0;
		while(elementsRead < length) {
    		if(closed) {
    			throw new EOFException();
    		}

    		if(buffer == null) {
    			buffer = fetchBuffer();
    			if(buffer == null && senderClosed) {
    				if (logger.isDebugEnabled()) {
    					logger.debug("senderClosed detected in read()");
    				}
    				closed = true;
    				cleanUp();
    				throw new EOFException();
           	 	}
    		} else if(buffer.remaining() == 0) {
    			MxReceiveBuffer.recycle(buffer);
    			buffer = null;
    			buffer = fetchBuffer();
    			if(buffer == null && senderClosed) {
    				if (logger.isDebugEnabled()) {
    					logger.debug("senderClosed detected in read()");
    				}
    				closed = true;
    				cleanUp();
    				throw new EOFException();
           	 	}
    		} else {
    			int r = buffer.readArray(destination, offset + elementsRead, length - elementsRead);
    			elementsRead += r;
    			bytesRead += r * SizeOf.LONG;
    		}
    	}		
	}

	@Override
	public void readArray(short[] destination, int offset, int length) throws IOException {
		int elementsRead = 0;
		while(elementsRead < length) {
    		if(closed) {
    			throw new EOFException();
    		}

    		if(buffer == null) {
    			buffer = fetchBuffer();
    			if(buffer == null && senderClosed) {
    				if (logger.isDebugEnabled()) {
    					logger.debug("senderClosed detected in read()");
    				}
    				closed = true;
    				cleanUp();
    				throw new EOFException();
           	 	}
    		} else if(buffer.remaining() == 0) {
    			MxReceiveBuffer.recycle(buffer);
    			buffer = null;
    			buffer = fetchBuffer();
    			if(buffer == null && senderClosed) {
    				if (logger.isDebugEnabled()) {
    					logger.debug("senderClosed detected in read()");
    				}
    				closed = true;
    				cleanUp();
    				throw new EOFException();
           	 	}
    		} else {
    			int r = buffer.readArray(destination, offset + elementsRead, length - elementsRead);
    			elementsRead += r;
    			bytesRead += r * SizeOf.SHORT;
    		}
    	}		
	}

	@Override
	public char readChar() throws IOException {
		while(!closed) {
        	try {
	        	while(buffer.remaining() == 0) {
	        		MxReceiveBuffer.recycle(buffer);
	    			buffer = null;
		        	buffer = fetchBuffer();
	        	}
	        	char result = buffer.readChar();
	        	bytesRead += SizeOf.CHAR;
	        	return result;
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
           	 		throw new EOFException();
           	 	}
	        }
        }
		throw new EOFException();
	}

	@Override
	public double readDouble() throws IOException {
		while(!closed) {
        	try {
	        	while(buffer.remaining() == 0) {
	        		MxReceiveBuffer.recycle(buffer);
	    			buffer = null;
		        	buffer = fetchBuffer();
	        	}
	        	double result = buffer.readDouble();
	        	bytesRead += SizeOf.DOUBLE;
	        	return result;
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
           	 		throw new EOFException();
           	 	}
	        }
        }
		throw new EOFException();	
	}

	@Override
	public float readFloat() throws IOException {
		while(!closed) {
        	try {
	        	while(buffer.remaining() == 0) {
	        		MxReceiveBuffer.recycle(buffer);
	    			buffer = null;
		        	buffer = fetchBuffer();
	        	}
	        	float result = buffer.readFloat();
	        	bytesRead += SizeOf.FLOAT;
	        	return result;
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
           	 		throw new EOFException();
           	 	}
	        }
        }
		throw new EOFException();
	}

	@Override
	public int readInt() throws IOException {
		while(!closed) {
        	try {
	        	while(buffer.remaining() == 0) {
	        		MxReceiveBuffer.recycle(buffer);
	    			buffer = null;
		        	buffer = fetchBuffer();
	        	}
	        	int result = buffer.readInt();
	        	bytesRead += SizeOf.INT;
	        	return result;
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
           	 		throw new EOFException();
           	 	}
	        }
        }
		throw new EOFException();	
	}

	@Override
	public long readLong() throws IOException {
		while(!closed) {
        	try {
	        	while(buffer.remaining() == 0) {
	        		MxReceiveBuffer.recycle(buffer);
	    			buffer = null;
		        	buffer = fetchBuffer();
	        	}
	        	long result = buffer.readLong();
	        	bytesRead += SizeOf.LONG;
	        	return result;
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
           	 		throw new EOFException();
           	 	}
	        }
        }
		throw new EOFException();	
	}

	@Override
	public short readShort() throws IOException {
		while(!closed) {
        	try {
	        	while(buffer.remaining() == 0) {
	        		MxReceiveBuffer.recycle(buffer);
	    			buffer = null;
		        	buffer = fetchBuffer();
	        	}
	        	short result = buffer.readShort();
	        	bytesRead += SizeOf.SHORT;
	        	return result;
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
           	 		throw new EOFException();
           	 	}
	        }
        }
		throw new EOFException();	
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
