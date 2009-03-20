package mxio.tests;

import java.io.IOException;

public class PingPong extends Common {

	PingPong() throws IOException {
		super();
	}

	@Override
	void doTest(boolean server) throws IOException {
		byte[] data = new byte[len];
		if (server) {
			System.out.println("PingPong " + count + " * " + len + " bytes");
		}
		
		for(int j = 0; j < retries; j++) {
			if (server) {
//				long oswrite = 0 , osflush = 0, isread = 0, other = 0;
				long nanotime = System.nanoTime();
//				other -= System.nanoTime();
				for(int i = 0; i < count; i++) {
//					oswrite -= System.nanoTime();
					os.write(data);
//					oswrite += System.nanoTime();
//					osflush -= System.nanoTime();
					os.flush();
//					osflush += System.nanoTime();
					int bytes = 0;
					while (bytes < len) {
//						other += System.nanoTime();
//						isread -= System.nanoTime();
						bytes += is.read(data, bytes, len - bytes);
//						isread += System.nanoTime();
//						other -= System.nanoTime();
					}
					
				}
//				other += System.nanoTime();
				nanotime = System.nanoTime() - nanotime;
				long millis = nanotime / 1000000; 
				System.out.println("" + count + " PingPongs took " + ((double)millis /1000) + " s");
				System.out.println("Roundtrip time: " + (double)millis/(double)count + " ms");
				System.out.println("Bandwidth: " + (((double)count * (double)len * 2) / (double)(1000*1000))/ ((double) millis / 1000) + " MB/s");
//				System.out.println("oswrite: " + (((double)oswrite / 1000)/count) + " micros");
//				System.out.println("osflush: " + (((double)osflush / 1000)/count) + " micros");
//				System.out.println("isread: " + (((double)isread / 1000)/count) + " micros");
//				System.out.println("other: " + (((double)other / 1000)/count) + " micros");
			} else {
//				long oswrite = 0 , osflush = 0, isread = 0;
//				long nanotime = System.nanoTime();
				
				for(int i = 0; i < count; i++) {
					int bytes = 0;
					while (bytes < len) {
//						isread -= System.nanoTime();
						bytes += is.read(data, bytes, len);
//						isread += System.nanoTime();
					}
//					oswrite -= System.nanoTime();
					os.write(data);
//					oswrite += System.nanoTime();
//					osflush -= System.nanoTime();
					os.flush();
//					osflush += System.nanoTime();
				}
				
//				nanotime = System.nanoTime() - nanotime;
//				long millis = nanotime / 1000000; 
//				System.out.println("" + count + " PingPongs took " + ((double)millis /1000) + " ms");
//				System.out.println("Roundtrip time: " + (double)millis/(double)count + " ms");
//				System.out.println("Bandwidth: " + (((double)count * (double)len * 2) / (double)(1000*1000))/ ((double) millis / 1000) + " MB/s");
//				System.out.println("oswrite: " + (((double)oswrite / 1000)/count) + " micros");
//				System.out.println("osflush: " + (((double)osflush / 1000)/count) + " micros");
//				System.out.println("isread: " + (((double)isread / 1000)/count) + " micros");
			}
		}
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {		
		init(args);
		new PingPong().run();
	}
}
