package mxio;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SelectableInputStream extends InputStream {
	
	private static final Logger logger = LoggerFactory
    .getLogger(SelectableInputStream.class);

		
	private	LinkedBlockingQueue<ReceiveBuffer> queue;

	private volatile boolean inSelector = false;

	private Selector selector;
	
	protected SelectableInputStream(MxSocket socket, MxAddress source,
			int endpointNumber, long matchData) throws IOException {
		super(socket, source, endpointNumber, matchData);

		if (logger.isDebugEnabled()) {
			logger.debug("SelectableInputStream <-- " + source.toString() + " : " + 
					Integer.toHexString(Matching.getPort(matchData)) 
					+  " created.");
		}
		queue = new LinkedBlockingQueue<ReceiveBuffer>();
	}
		
	protected void newMessage(ReceiveBuffer buf) {
		while(true) {
			try {
				queue.put(buf);
				if(selector != null && inSelector == false) {
					inSelector = true;
					selector.ready(this);
				}
				return;
			} catch (InterruptedException e) {
				// TODO ignore?
			}
		}
	}
	
	@Override
	protected ReceiveBuffer fetchBuffer() throws IOException {
		ReceiveBuffer buffer = null;
		
		while(buffer == null) {
			if(senderClosed) {
				try {
					buffer = queue.poll(100, TimeUnit.MILLISECONDS);
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
	
	
	protected void isSelected() {
		selector = null;
		inSelector = false;
	}
	
	
	@Override
	public boolean isSelectableStream() {
		return true;
	}
	
	@Override
	public boolean attach(Selector s) {
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
	public void detach() {
		selector = null;
		inSelector = false;
	}

	@Override
	public void close() throws IOException {
		if(selector != null && inSelector == false) {
			selector.ready(this);
			inSelector = true;
		}
		super.close();
	}

	@Override
	protected void senderClosedConnection() {
		super.senderClosedConnection();
		// let the user find out that we are closed
		if(selector != null && inSelector == false) {
			selector.ready(this);
			inSelector = true;
		}
	}

	@Override
	protected void cleanUp() {
		ReceiveBuffer buffer = queue.poll();
		while(buffer != null) {
			if(!buffer.cancel()) {
				try {
					buffer.finish(1, true);
				} catch (IOException e) {
					// TODO ignore
				}
			}
			ReceiveBuffer.recycle(buffer);
			buffer = queue.poll();
		}
		// let the user find out that we are closed
		if(selector != null && inSelector == false) {
			selector.ready(this);
		}
		detach();
	}
}
