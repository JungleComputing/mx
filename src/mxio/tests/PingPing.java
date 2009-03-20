package mxio.tests;

import java.io.IOException;


public class PingPing extends Common

{	
	PingPing() throws IOException {
		super();
	}

	void doTest(boolean server) throws IOException {
		byte[] data = new byte[len];
		if (server) {
			System.out.println("PingPing " + count + " * " + len + " bytes");
		}

		for(int j = 0; j < retries; j++) {
			long time = System.currentTimeMillis();
			for(int i = 0; i < count; i++) {
				os.write(data);
				os.flush();
				int bytes = 0;
				while (bytes < len) {
					bytes += is.read(data, bytes, len - bytes);
				}
			}
			time = System.currentTimeMillis() - time;
			if(server) {
				System.out.println("" + count + " PingPings took " + ((double)time / 1000) + " s");
				System.out.println("One-way latency: " + (double)time/(double)count + " ms");
				System.out.println("One-way Bandwidth: " + (((double)count * (double)len) / (double)(1000*1000))/ ((double) time / 1000) + " MiB/s");
			}
		}
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {		
		init(args);
		new PingPing().run();
	}
}
