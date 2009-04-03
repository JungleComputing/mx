package mxio.tests;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;

import mxio.*;


public abstract class Common implements MxListener {
	static int len         = 8 * 1024; // 4 or more
	static int count       = 10000;
	static int retries     = 10;
	
	MxSocket socket;
	DataInputStream is = null;
	DataOutputStream os = null;
	MxAddress sender;
	
	Common() {
		try {
			socket = new MxSocket(this);
		} catch (MxException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public final void newConnection(ConnectionRequest request) {
		CharBuffer cbuf = ByteBuffer.wrap(request.getDescriptor()).order(ByteOrder.BIG_ENDIAN).asCharBuffer();
		System.err.println("New request arrived: " + cbuf.toString());
		
		if(is != null) {
			request.reject();
		} else {
			String str = "Hi There!";
			byte[] msg = new byte[2*str.length()];
			ByteBuffer buf = ByteBuffer.wrap(msg).order(ByteOrder.BIG_ENDIAN);
			buf.asCharBuffer().put(str);
			request.setReplyMessage(msg);
			is = request.accept(false);
			if(is != null) {
				sender = request.getSourceAddress();
				synchronized(this) {
					notifyAll();
				}
			}
		}
	}
	
	private final void sanity(boolean server) throws IOException {
		int first = 3;
		int second = 5;
		
		if(server) {
			System.err.println("Server sanity:");
			os.write(first);
			os.flush();
			System.err.println("S1...");
			if (is.read() != first) {
				System.err.println("Server insane");
				System.exit(1);
			}
			System.err.println("S2...");
			os.write(second);
			os.flush();
			System.err.println("S3...");
			if (is.read() != second) {
				System.err.println("Server insane");
				System.exit(1);
			}
			System.err.println("Server sane :-)");
		} else {
			int result;
			System.err.println("Client sanity:");
			result = is.read(); 
			if (result != first) {
				System.err.println("Client insane");
				System.exit(1);
			}
			System.err.println("C1...");

			os.write(result);
			os.flush();
			System.err.println("C2...");
			result = is.read();
			if (result != second) {
				System.err.println("Client insane");
				System.exit(1);
			}
			System.err.println("C3...");
			os.write(result);
			os.flush();
			System.err.println("Client sane :-)");
		}
	}
	
	abstract void doTest(boolean server) throws IOException;
	
	private final void client() throws IOException {
		String hostnames;
        String servername;
                                   
        try { // give the server some time to set up the link 
                Thread.sleep(1000);
        } catch (InterruptedException e) {
                //nothing
        }
        /* find the server address */
        /* on the DAS3 of the form nodeXXX:0 */
        hostnames = System.getenv("PRUN_HOSTNAMES");
        if (hostnames == null) {
                System.err.println("no hostenv");
                System.exit(1);
        }
        servername = hostnames.substring(0, 7) + ":0";
        
		MxAddress target = new MxAddress(servername, 0);
		
		try {
			String str = "Hello server!";
			byte[] ds = new byte[2*str.length()];
			ByteBuffer buf = ByteBuffer.wrap(ds).order(ByteOrder.BIG_ENDIAN);
			buf.asCharBuffer().put(str);
			
			Connection conn = socket.connect(target, ds, 1000);
			CharBuffer cbuf = ByteBuffer.wrap(conn.getReplyMessage()).order(ByteOrder.BIG_ENDIAN).asCharBuffer();
			System.err.println("Reply arrived: " + cbuf.toString());
			os = conn.getDataOutputStream();
		} catch (MxException e) {
			e.printStackTrace();
			System.exit(2);
		}
		
		while(is == null) {
			synchronized(this) {
				try {
					wait();
				} catch (InterruptedException e) {
					// do nothing
				}
			}
		}
		System.out.println("Client connected");
		
		sanity(false);
		doTest(false);
		sanity(false);
		
		os.close();
		is.close();
	}

	private final void server() throws IOException {
		while(is == null) {
			synchronized(this) {
				try {
					wait();
				} catch (InterruptedException e) {
					// do nothing
				}
			}
		}
		
		try {
			String str = "Hello client!";
			byte[] ds = new byte[2*str.length()];
			ByteBuffer buf = ByteBuffer.wrap(ds).order(ByteOrder.BIG_ENDIAN);
			buf.asCharBuffer().put(str);
			Connection conn = socket.connect(sender, ds, 1000);
			CharBuffer cbuf = ByteBuffer.wrap(conn.getReplyMessage()).order(ByteOrder.BIG_ENDIAN).asCharBuffer();
			System.err.println("Reply arrived: " + cbuf.toString());
			os = conn.getDataOutputStream();
		} catch (MxException e) {
			e.printStackTrace();
			System.exit(2);
		}
		
		System.out.println("Server connected");

		sanity(true);
		doTest(true);
		sanity(true);
		
		os.close();
		is.close();
	}

	protected final void run() throws IOException {
		String rank = System.getenv("PRUN_CPU_RANK");
        if(rank == null) {
                System.err.println("no rank env");
                return;
        }
        if(rank.equals("0")) {
        	// server
			server();
			System.err.println("Server finished");
		} else {
			client();
			System.err.println("Client finished");
		}	
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	protected static void init(String[] args) throws IOException {
		int i = 0;

	    while (i < args.length) { 
			if (false) {
			} else if (args[i].equals("-len")) { 
			    len = Integer.parseInt(args[i+1]);
			    i += 2;
			} else if (args[i].equals("-count")) { 
			    count = Integer.parseInt(args[i+1]);
			    i += 2;
			} else if (args[i].equals("-retries")) { 
			    retries = Integer.parseInt(args[i+1]);
			    i += 2;
			} else {
			    System.err.println("unknown option: " + args[i]);
			    System.exit(1);
			}
	    }
	}
	
}
