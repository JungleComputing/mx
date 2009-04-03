package mxio;

public final class Connection {
	public static final byte ACCEPT = 1;
	public static final byte REJECT = 2;

	private DataOutputStream os;
	private byte[] message;
	private byte reply;

	Connection(DataOutputStream os, byte reply, byte[] message) {
		this.message = message;
		this.os = os;
		this.reply = reply;
	}

	public DataOutputStream getDataOutputStream() {
		return os;
	}

	public byte[] getReplyMessage() {
		return message;
	}

	public byte getReply() {
		return reply;
	}
}