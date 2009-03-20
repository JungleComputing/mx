package mxio;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Selector {
	
	static final Logger logger = LoggerFactory
    .getLogger(Selector.class);
	
	LinkedBlockingQueue<SelectableInputStream> queue;
	
	public Selector() {
		queue = new LinkedBlockingQueue<SelectableInputStream>();
	}
	
	public SelectableInputStream select() {
		SelectableInputStream s = null;
		do {
			try {
				s = queue.take();
			} catch (InterruptedException e) {
				// ignore
			}
		} while (s == null);
		
		if(logger.isDebugEnabled()) {
			logger.debug("Connection selected. Left in queue: " + queue.size());
		}
		s.isSelected();
		return s;
	}
	
	public SelectableInputStream select(long timeout) {
		if(logger.isDebugEnabled()) {
			logger.debug("select(t)");
		}
		SelectableInputStream s = null;
		try {
			s = queue.poll(timeout, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			// ignore
		}
		if(s == null) {
			if(logger.isDebugEnabled()) {
				logger.debug("No connection selected. Left in queue: " + queue.size());
			}
			return null;
		}
		if(logger.isDebugEnabled()) {
			logger.debug("Connection selected. Left in queue: " + queue.size());
		}
		s.isSelected();
		return s;
	}
	
	public SelectableInputStream poll() {
		SelectableInputStream s;
		s = queue.poll();
		if(s == null) {
			return null;
		}
		if(logger.isDebugEnabled()) {
			logger.debug("Connection selected. Left in queue: " + queue.size());
		}
		s.isSelected();
		return s;
	}
		
	void ready(SelectableInputStream s) {
		while(true) {
			try {
				queue.put(s);
				if(logger.isDebugEnabled()) {
					logger.debug("New ready Connection. Total: " + queue.size());
				}
				return;
			} catch (InterruptedException e) {
				// ignore
			}
		}
	}
}
