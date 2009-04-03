package ibis.ipl.impl.mx;

import ibis.ipl.PortType;
import ibis.ipl.SendPortDisconnectUpcall;
import ibis.ipl.impl.Ibis;
import ibis.ipl.impl.ReceivePortIdentifier;
import ibis.ipl.impl.SendPortConnectionInfo;
import ibis.ipl.impl.WriteMessage;

import java.io.IOException;
import java.util.Properties;

import mxio.CollectedWriteException;
import mxio.MulticastDataOutputStream;
import mxio.DataOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class MxMulticastSendPort extends MxSendPort {

	static final Logger logger = LoggerFactory
    .getLogger(MxMulticastSendPort.class);
	
    private class Conn extends SendPortConnectionInfo {
    	DataOutputStream os;

        Conn(DataOutputStream os, MxMulticastSendPort port, ReceivePortIdentifier target)
                throws IOException {
            super(port, target);
            this.os = os;
            mcos.add(os);
        }

        public void closeConnection() {
            try {
                os.close();
            } catch (Throwable e) {
                // ignored
            } finally {
                try {
                    mcos.remove(os);
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    //final OutputStreamSplitter splitter;
    final MulticastDataOutputStream mcos;

    MxMulticastSendPort(Ibis ibis, PortType type, String name,
            SendPortDisconnectUpcall cU, Properties props) throws IOException {
        super(ibis, type, name, cU, props);
        
        //TODO bufsize
        mcos = new MulticastDataOutputStream();
        initStream(mcos);
    }

    protected long totalWritten() {
        return mcos.bytesWritten();
    }

    protected void resetWritten() {
        mcos.resetBytesWritten();
    }

    protected SendPortConnectionInfo doConnect(ReceivePortIdentifier receiver,
            long timeoutMillis, boolean fillTimeout) throws IOException {

    	DataOutputStream os = 
    		((MxIbis) ibis).connect(this, receiver, (int) timeoutMillis,
                fillTimeout);
        Conn c = new Conn(os, this, receiver);
        if (out != null) {
            out.writeByte(NEW_RECEIVER);
        }
        initStream(mcos);
        return c;
    }

    protected void announceNewMessage() throws IOException {
    	if (logger.isDebugEnabled()) {
            logger.debug("Announcing new message");
        }
        out.writeByte(NEW_MESSAGE);
        if (type.hasCapability(PortType.COMMUNICATION_NUMBERED)) {
            out.writeLong(ibis.registry().getSequenceNumber(name));
        }
    }

    protected void handleSendException(WriteMessage w, IOException x) {
        ReceivePortIdentifier[] ports = null;
        synchronized (this) {
            ports = receivers.keySet()
                            .toArray(new ReceivePortIdentifier[0]);
        }

        if (x instanceof CollectedWriteException) {
        	CollectedWriteException e = (CollectedWriteException) x;

            Exception[] exceptions = e.getExceptions();
            mxio.DataOutputStream[] streams = e.getStreams();

            for (int i = 0; i < ports.length; i++) {
                Conn c = (Conn) getInfo(ports[i]);
                for (int j = 0; j < streams.length; j++) {
                    if (c.os == streams[j]) {
                        lostConnection(ports[i], exceptions[j]);
                        break;
                    }
                }
            }
        } else {
            // Just close all connections. ???
            for (int i = 0; i < ports.length; i++) {
                lostConnection(ports[i], x);
            }
        }
    }

    protected void closePort() {

        try {
            out.close();
            mcos.close();
        } catch (Throwable e) {
            // ignored
        }

        out = null;
    }

}
