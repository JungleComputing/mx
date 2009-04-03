package mxio;

interface Config {	
	//'NIO' streams

	/**
     * The header contains 1 byte for the byte order, one byte indicating the
     * length of the padding at the end of the packet (in bytes), followed by 
     * two unused bytes. Then there is 1 int (4 bytes) for the number of bytes
     * sent (in bytes!), followed by four bytes containing the port number and 
     * 4 empty bytes for alignment. 
     * 
     */
	static final int SIZEOF_HEADER = 8;
	static final int BYTEORDER_BYTE = 0;
	static final int PORT_BYTE = 4;
			
    /** Byte buffer size used. **/
	//static final int START_BUFFER_SIZE = 2 * 1024;
    static final int BUFFER_SIZE = 64 * 1024;
    static final int REPORTED_BUFFER_SIZE = 8 * 1024;

	static final int DELIVERY_THREAD_BUFFERS = 8;
	
	/** Receive queue size of LowLatencyInputStream **/
	static final int RECEIVE_QUEUE_SIZE = 4;
	
	/** Flush queue size of OutputStreams **/
	static final int FLUSH_QUEUE_SIZE = 4;
	
	/** Amount of poll before a blocking call to complete a request **/ 
	static final int POLLS = 2000 ; // 5 polls per micro? probably less
	static final int ACK_INTERVAL = 5;
}
