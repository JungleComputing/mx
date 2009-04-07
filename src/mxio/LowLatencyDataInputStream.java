package mxio;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LowLatencyDataInputStream extends DataInputStream {

	private static final Logger logger = LoggerFactory
    .getLogger(LowLatencyDataInputStream.class);
	
	private	ArrayBlockingQueue<MxReceiveBuffer> queue;

	protected LowLatencyDataInputStream(MxSocket socket, MxAddress source,
			int endpointNumber, long matchData) throws IOException {
		super(socket, source, endpointNumber, matchData);

		if (logger.isDebugEnabled()) {
			logger.debug("LowLatencyInputStream <-- " + source.toString() + " : " + 
					Integer.toHexString(Matching.getPort(matchData)) 
					+  " created.");
		}
		queue = new ArrayBlockingQueue<MxReceiveBuffer>(Config.RECEIVE_QUEUE_SIZE);

		for(int i = 0; i < Config.RECEIVE_QUEUE_SIZE; i++) {
			postBuffer();
		}

	}

	@Override
	protected MxReceiveBuffer fetchBuffer() throws IOException {
		MxReceiveBuffer buffer = null;

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
					MxReceiveBuffer.recycle(buffer);
					buffer = queue.poll();
				} while (buffer != null);
				closed = true;
				cleanUp();
				return null;
			}
		}
		// add a new buffer to the receive queue
		postBuffer();
		/*
		if(++ackCounter >= ACK_INTERVAL) {
			ackCounter = 0;
			socket.sendAck(this);
		}
		*/
		return buffer;
	}

	private void postBuffer() {
		MxReceiveBuffer buffer = MxReceiveBuffer.get();
		try {
			buffer.post(endpointNumber, matchData, Matching.MASK_ALL);
		} catch (IOException e) {
			MxReceiveBuffer.recycle(buffer);
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		queue.add(buffer);
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
	}
}
