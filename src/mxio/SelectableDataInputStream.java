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

	private boolean markedAsReady = false;

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
				notifySelector();
				return;
			} catch (InterruptedException e) {
				// TODO ignore?
			}
		}
	}
	
	private synchronized void notifySelector() {
		if(selector != null && markedAsReady == false) {
			markedAsReady = true;
			selector.ready(this);
			
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
					// ignore
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
					// ignore
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
		markedAsReady = false;
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
			notifySelector();
			return true;
		}
		if(!queue.isEmpty() || available() > 0)	{
			notifySelector();
			return true;
		} else {
			markedAsReady = false;
			return true;
		}
	}

	@Override
	public synchronized void detach() {
		selector = null;
		markedAsReady = false;
	}

	@Override
	public synchronized void close() throws IOException {
		if(selector != null && markedAsReady == false) {
			notifySelector();
		}
		super.close();
	}

	@Override
	protected synchronized void senderClosedConnection() {
		super.senderClosedConnection();
		// let the user find out that we are closed
		if(selector != null && markedAsReady == false) {
			notifySelector();
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
		if(selector != null && markedAsReady == false) {
			notifySelector();
		}
		detach();
	}
}
