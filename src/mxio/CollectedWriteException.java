package mxio;

import java.io.IOException;

import java.util.ArrayList;

public class CollectedWriteException extends IOException {
	// based on splitterException from ibis.io

	/**
	 * 
	 */
	private static final long serialVersionUID = 8852201113944561908L;

	private ArrayList<OutputStream> oSes = new ArrayList<OutputStream>();

	private ArrayList<Exception> exceptions = new ArrayList<Exception>();

	public CollectedWriteException() {
		// empty constructor
	}

	public void add(OutputStream c, Exception e) {
		if (oSes.contains(c)) {
			System.err.println("AAA, stream was already in splitter exception");
		}

		oSes.add(c);
		exceptions.add(e);
	}

	public int count() {
		return oSes.size();
	}

	public OutputStream[] getStreams() {
		return oSes.toArray(new OutputStream[0]);
	}

	public Exception[] getExceptions() {
		return exceptions.toArray(new Exception[0]);
	}

	public OutputStream getOS(int pos) {
		return oSes.get(pos);
	}

	public Exception getException(int pos) {
		return exceptions.get(pos);
	}

	public String toString() {
		String res = "got " + oSes.size() + " exceptions: ";
		for (int i = 0; i < oSes.size(); i++) {
			res += "   " + exceptions.get(i) + "\n";
		}

		return res;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Throwable#printStackTrace()
	 */
	public void printStackTrace() {
		for (int i = 0; i < oSes.size(); i++) {
			System.err.println("Exception: " + exceptions.get(i));
			((Exception) exceptions.get(i)).printStackTrace();
		}
	}
}
