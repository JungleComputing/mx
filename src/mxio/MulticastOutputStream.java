package mxio;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MulticastOutputStream extends OutputStream {
	
	private static final Logger logger = LoggerFactory
    .getLogger(MulticastOutputStream.class);
	
	static final int INITIAL_CONNECTIONS_SIZE = 8;
	
	private OutputStream[] connections = new OutputStream[INITIAL_CONNECTIONS_SIZE];
	int nrOfConnections = 0;
	
	public MulticastOutputStream() {
		super();
	}

	@Override
	long doSend(SendBuffer buffer) throws IOException {
        if (logger.isDebugEnabled()) {
        	logger.debug("doSend");
		}
		if(nrOfConnections == 0) {
			throw new IOException("MulticastStream not connected to other Streams.");
		}
		
		long result = buffer.remaining();
		CollectedWriteException cwe = null;
		if(nrOfConnections == 1) {
			connections[0].doSend(buffer);
		} else {
			SendBuffer[] copies = SendBuffer.replicate(buffer, nrOfConnections);
			for(int i = 0; i < nrOfConnections; i++) {
				try {
				connections[i].doSend(copies[i]);
				SendBuffer.recycle(buffer);
				} catch (IOException e) {
					if(cwe == null) {
						cwe = new CollectedWriteException();
					}
					cwe.add(connections[i], e);
					try {
						doRemove(connections[i]);
					} catch (IOException e2) {
						//ignore
					}	
				}
			}
		}if(cwe != null) {
			throw cwe; //FIXME what about the return value??
		}
		return result;
	}
	
	@Override
	void doFlush() throws CollectedWriteException {
        if (logger.isDebugEnabled()) {
        	logger.debug("doFlush");
		}
		CollectedWriteException cwe = null;
		for(int i = 0; i < nrOfConnections; i++) {
			try {
				connections[i].flush();
			} catch (IOException e) {
				if(cwe == null) {
					cwe = new CollectedWriteException();
				}
				cwe.add(connections[i], new ClosedChannelException());
				try {
					doRemove(connections[i]);
				} catch (IOException e2) {
					//ignore
				}
			}
		}
		if(cwe != null) {
			throw cwe;
		}
	}
	
	@Override
	void doClose() {
		for(int i = 0; i < nrOfConnections; i++) {
			try {
				connections[i].close();
			} catch(IOException e) {
				//ignore
			}
			connections[i] = null;
		}
		nrOfConnections = 0;
	}

	public final void add(OutputStream connection) throws IOException {
		// end all current transfers
		flush();
	
		if (nrOfConnections == connections.length) {
			OutputStream[] newConnections = new OutputStream[connections.length * 2];
            for (int i = 0; i < connections.length; i++) {
                newConnections[i] = connections[i];
            }
            connections = newConnections;
        }
		connection.addToMulticast(this);
		
        connections[nrOfConnections] = connection;
        if (logger.isDebugEnabled()) {
        	logger.debug("Connection added at position " + nrOfConnections);
		}
        nrOfConnections++;
	}

	public final void remove(OutputStream connection) throws IOException {
		flush();
		if (logger.isDebugEnabled()) {
			logger.debug("remove");
		}

		doRemove(connection);
    }
	
	private final void doRemove(OutputStream connection) throws IOException {
        for (int i = 0; i < nrOfConnections; i++) {
            if (connections[i] == connection) {
                if (logger.isDebugEnabled()) {
                	logger.debug("Connection removed at position " + i);
                }
                connections[i].removeFromMulticast();
                nrOfConnections--;
                connections[i] = connections[nrOfConnections];
                connections[nrOfConnections] = null;
                return;
            }
        }
        
        throw new IOException("tried to remove a connection that was not a member");
    }	
	
	@Override
	public String toString() {
		String result = "MulticastOutputStream: {";
		for(OutputStream os: connections) {
			result += " <" + os.toString() + ">";
		}
		result += " }";
		return result;
	}
	
}
