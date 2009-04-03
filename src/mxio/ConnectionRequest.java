package mxio;

public class ConnectionRequest {
	protected static final int PENDING = 0;
	protected static final int ACCEPTED = 1;
	protected static final int REJECTED = 2;
	
	
	private final MxAddress source;
	private final byte[] descriptor;
	
	protected int status = PENDING;
	protected byte[] replyMessage;
	protected int msgSize = 0;
	private MxSocket socket;
	private boolean selectable = true;
	
	protected ConnectionRequest(MxSocket socket, MxAddress source, byte[] descriptor) {
		this.socket = socket;
		this.source = source;
		this.descriptor = descriptor;
		replyMessage = new byte[MxSocket.MAX_CONNECT_MSG_SIZE];
	}

	public MxAddress getSourceAddress() {
		return source;
	}
	
	public byte[] getDescriptor() {
		return descriptor;
	}
	
	public void reject() {
		status = REJECTED;
	}
	
	public DataInputStream accept(boolean selectable) {
		status = ACCEPTED;
		this.selectable = selectable;		
		try {
			return socket.accept(this);	
		} catch (MxException e) {
			// TODO handle this
			reject();
			return null;
		}		
	}
	
	/**
	 * Set a detailed reply message to the request. When the message is larger than 
	 * MxSocket.MAX_CONNECT_MSG_SIZE the message is truncated.
	 * @param message the message
	 */
	public void setReplyMessage(byte[] message) {
		if(message.length > this.replyMessage.length) {
			msgSize = this.replyMessage.length;
		} else {
			msgSize = message.length;
		}
		System.arraycopy(message, 0, replyMessage, 0, msgSize);
	}

	protected boolean selectable() {
		return selectable;
	}
}
