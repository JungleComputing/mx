package mxio.tests;

import java.io.IOException;


public class Stream extends Common {

	Stream() throws IOException {
		super();
	}

	@Override
	void doTest(boolean server) throws IOException {
		byte[] data = new byte[len];
		if (server) {
			System.out.println("Stream " + count + " * " + len + " bytes");
		}
		for(int j = 0; j < retries; j++) {
			if (server) {
				is.read();
				long time = System.currentTimeMillis();
				for(int i = 0; i < count; i++) {
					os.write(data);
					os.flush();
				}

				is.read();
				time = System.currentTimeMillis() - time;

				System.out.println("Bandwidth: " + (((double)count * (double)len) / (double)(1000*1000))/ ((double) time / 1000) + " MB/s");
			} else {
				os.write(1);
				os.flush();
				for(int i = 0; i < count; i++) {
					int bytes = 0;
					while (bytes < len) {
						bytes += is.read(data, bytes, len - bytes);
					}
				}
				os.write(0);
				os.flush();
			}
		}
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {		
		init(args);
		new Stream().run();
	}

}
