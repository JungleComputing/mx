package mxio;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SelectableDataInputStream extends DataInputStream {
	
	private static final Logger logger = LoggerFactory
    .getLogger(SelectableDataInputStream.class);
		
	private	LinkedBlockingQueue<MxReceiveBuffer> queue;

	private boolean inSelector = false;

	private Selector selector;
	
	protected SelectableDataInputStream(MxSocket socket, MxAddress source,
			int endpointNumber, long matchData) throws IOException {
		super(socket, source, endpointNumber, matchData);

		if (logger.isDebugEnabled()) {
			logger.debug("SelectableInputStream <-- " + source.toString() + " : " + 
					Integer.toHexString(Matching.getPort(matchData)) 
					+  " created.");
		}
		queue = new LinkedBlockingQueue<MxReceiveBuffer>();
	}
		
	protected void newMessage(MxReceiveBuffer buf) {
		while(true) {
			try {
				queue.put(buf);
				synchronized(this) {
					if(selector != null && inSelector == false) {
						inSelector = true;
						selector.ready(this);
					}
				}
				return;
			} catch (InterruptedException e) {
				// TODO ignore?
			}
		}
	}
	
	@Override
	protected MxReceiveBuffer fetchBuffer() throws IOException {
		MxReceiveBuffer buffer = null;
		
		while(buffer == null) {
			if(senderClosed) {
				try {
					buffer = queue.poll(1000, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					// TODO ignore
				}
				if(buffer == null) {
					closed = true;
					cleanUp();
					return null;
				}
				// always return here, all new message are already here
			} else {
				try {
					buffer = queue.poll(1000, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					// TODO ignore
				}	
			}
		}
		return buffer;
	}
	
	@Override
	public boolean attachedToSelector() {
		return selector != null;
	}
	
	
	protected synchronized void isSelected() {
		selector = null;
		inSelector = false;
	}
	
	
	@Override
	public boolean isSelectableStream() {
		return true;
	}
	
	@Override
	public synchronized boolean attach(Selector s) {
		selector = s;
		if(s == null) {
			return false;
		}
		if(closed || senderClosed) {
			selector.ready(this);
			inSelector = true;
			return true;
		}
		
		if(!queue.isEmpty() || available() > 0)	{
			selector.ready(this);
			inSelector = true;
			return true;
		} else {
			inSelector = false;
			return true;
		}
	}

	@Override
	public synchronized void detach() {
		selector = null;
		inSelector = false;
	}

	@Override
	public synchronized void close() throws IOException {
		if(selector != null && inSelector == false) {
			selector.ready(this);
			inSelector = true;
		}
		super.close();
	}

	@Override
	protected synchronized void senderClosedConnection() {
		super.senderClosedConnection();
		// let the user find out that we are closed
		if(selector != null && inSelector == false) {
			selector.ready(this);
			inSelector = true;
		}
	}

	@Override
	protected void cleanUp() {
		MxReceiveBuffer buffer = queue.poll();
		while(buffer != null) {
			if(!buffer.cancel()) {
				try {
					buffer.finish(1, true);
				} catch (IOException e) {
					// TODO ignore
				}
			}
			MxReceiveBuffer.recycle(buffer);
			buffer = queue.poll();
		}
		// let the user find out that we are closed
		if(selector != null && inSelector == false) {
			selector.ready(this);
		}
		detach();
	}
}
