package mxio;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LowLatencyInputStream extends InputStream {

	private static final Logger logger = LoggerFactory
    .getLogger(LowLatencyInputStream.class);
	
	private	ArrayBlockingQueue<ReceiveBuffer> queue;

	protected LowLatencyInputStream(MxSocket socket, MxAddress source,
			int endpointNumber, long matchData) throws IOException {
		super(socket, source, endpointNumber, matchData);

		if (logger.isDebugEnabled()) {
			logger.debug("LowLatencyInputStream <-- " + source.toString() + " : " + 
					Integer.toHexString(Matching.getPort(matchData)) 
					+  " created.");
		}
		queue = new ArrayBlockingQueue<ReceiveBuffer>(Config.FLUSH_QUEUE_SIZE);

		for(int i = 0; i < Config.FLUSH_QUEUE_SIZE; i++) {
			postBuffer();
		}

	}

	@Override
	protected ReceiveBuffer fetchBuffer() throws IOException {
		ReceiveBuffer buffer = null;

		buffer = queue.poll();
		
		while(buffer == null) {
			for(int i = 0; i < queue.remainingCapacity(); i++) {
				postBuffer();
			}
			buffer = queue.poll();
		}


		while(!buffer.finish(1000, true)) {
			if(senderClosed) {
				do {
					if(buffer.cancel() == false) {
						buffer.finish(1, true); // should finish immediately
						return buffer;
					}
					ReceiveBuffer.recycle(buffer);
					buffer = queue.poll();
				} while (buffer != null);
				closed = true;
				cleanUp();
				return null;
			}
		}
		// add a new buffer to the receive queue
		postBuffer();
		return buffer;
	}

	private void postBuffer() {
		ReceiveBuffer buffer = ReceiveBuffer.get();
		try {
			buffer.post(endpointNumber, matchData, Matching.MASK_ALL);
		} catch (IOException e) {
			ReceiveBuffer.recycle(buffer);
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		queue.add(buffer);
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
	}
}
