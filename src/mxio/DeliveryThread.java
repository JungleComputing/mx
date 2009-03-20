package mxio;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public final class DeliveryThread implements Runnable, Config {
	
	private static final Logger logger = LoggerFactory
    .getLogger(DeliveryThread.class);
	
	ArrayBlockingQueue<ReceiveBuffer> queue;
	int capacity;
	int endpointNumber;
	MxSocket socket;
	
	private static long SELECTABLEMASK = Matching.construct(Matching.PROTOCOL_MASK, Matching.getPort(Matching.SELECTABLEPORTS)); 
	private static long SELECTABLEDATA = Matching.construct(Matching.PROTOCOL_DATA, Matching.getPort(Matching.SELECTABLEPORTS));
	
	boolean open = true;
	
	DeliveryThread(MxSocket socket, int capacity) {
		this.capacity = capacity;
		this.socket = socket;
		this.endpointNumber = socket.endpointNumber();
		
		queue = new ArrayBlockingQueue<ReceiveBuffer>(capacity);
		
	}

	 void close() {
		open = false;
	}
	
	public void run() {
		for(int i = 0; i< capacity; i++) {
			postBuffer();
		}

		while(open) {
			ReceiveBuffer buf = null;
			while(buf == null) {
				try {
					buf = queue.poll(1, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					// TODO ignore
				}
				if(!open) {
					finish();
					return;
				}
			}
			
			try {
				while(!buf.finish(1000, true)) {
					if(!open) {
						finish();
						return;
					}
				}
			} catch (IOException e) {
				// something is seriously wrong here: we crash
				// TODO crash a little less drastically 
				e.printStackTrace();
				System.exit(1);
			}
			
			
			SelectableInputStream target = socket.getSelectableInputStream(buf.port());
			if(target == null) {
				if(logger.isDebugEnabled()) {
					logger.debug("Buffer dropped: unknown receiver: " + buf.port());
				}
				//target stream unknown, drop message?
			} else {
				target.newMessage(buf);
			}
			postBuffer();			
		}
	}

	private void finish() {
		ReceiveBuffer buffer = queue.poll();
		while(buffer != null) {
			buffer.cancel();
			ReceiveBuffer.recycle(buffer);
			buffer = queue.poll();
		}
	}

	private void postBuffer() {
		ReceiveBuffer buf = ReceiveBuffer.get();
		if (queue.offer(buf) == false) {
			if(logger.isDebugEnabled()) {
				logger.debug("not posting buffer: queue full");
			}
			// Queue full
			ReceiveBuffer.recycle(buf);
			return;
		}
				
		try {
			buf.post(endpointNumber, SELECTABLEDATA, SELECTABLEMASK);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
