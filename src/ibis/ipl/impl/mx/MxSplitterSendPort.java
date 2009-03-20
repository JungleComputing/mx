package ibis.ipl.impl.mx;

import ibis.io.BufferedArrayOutputStream;
import ibis.io.OutputStreamSplitter;
import ibis.io.SplitterException;
import ibis.ipl.PortType;
import ibis.ipl.SendPortDisconnectUpcall;
import ibis.ipl.impl.Ibis;
import ibis.ipl.impl.ReceivePortIdentifier;
import ibis.ipl.impl.SendPortConnectionInfo;
import ibis.ipl.impl.WriteMessage;

import java.io.IOException;
import java.util.Properties;

import mxio.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class MxSplitterSendPort extends MxSendPort {

	static final Logger logger = LoggerFactory
    .getLogger(MxSplitterSendPort.class);
	
    private class Conn extends SendPortConnectionInfo {
    	OutputStream os;

        Conn(OutputStream os, MxSplitterSendPort port, ReceivePortIdentifier target)
                throws IOException {
            super(port, target);
            this.os = os;
            splitter.add(os);
        }

        public void closeConnection() {
            try {
                os.close();
            } catch (Throwable e) {
                // ignored
            } finally {
                try {
                    splitter.remove(os);
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    final OutputStreamSplitter splitter;

    final BufferedArrayOutputStream bufferedStream;

    MxSplitterSendPort(Ibis ibis, PortType type, String name,
            SendPortDisconnectUpcall cU, Properties props) throws IOException {
        super(ibis, type, name, cU, props);
        splitter =
                new OutputStreamSplitter(
                        !type.hasCapability(PortType.CONNECTION_ONE_TO_ONE)
                                && !type.hasCapability(
                                    PortType.CONNECTION_MANY_TO_ONE),
                        type.hasCapability(PortType.CONNECTION_ONE_TO_MANY) 
                                || type.hasCapability(
                                    PortType.CONNECTION_MANY_TO_MANY));
        bufferedStream = new BufferedArrayOutputStream(splitter, BUFSIZE);    

        initStream(bufferedStream);
    }

    protected long totalWritten() {
        return splitter.bytesWritten();
    }

    protected void resetWritten() {
        splitter.resetBytesWritten();
    }

    protected SendPortConnectionInfo doConnect(ReceivePortIdentifier receiver,
            long timeoutMillis, boolean fillTimeout) throws IOException {

    	OutputStream os = 
    		((MxIbis) ibis).connect(this, receiver, (int) timeoutMillis,
                fillTimeout);
        Conn c = new Conn(os, this, receiver);
        if (out != null) {
            out.writeByte(NEW_RECEIVER);
        }
        initStream(bufferedStream);
        return c;
    }

    protected void sendDisconnectMessage(ReceivePortIdentifier receiver,
            SendPortConnectionInfo conn) throws IOException {
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

    
    protected synchronized void finishMessage(WriteMessage w, long cnt)
            throws IOException {
        if (type.hasCapability(PortType.CONNECTION_ONE_TO_MANY)
                || type.hasCapability(PortType.CONNECTION_MANY_TO_MANY)) {
            // exception may have been saved by the splitter. Get them
            // now.
            SplitterException e = splitter.getExceptions();
            if (e != null) {
                gotSendException(w, e);
            }
        }
        super.finishMessage(w, cnt);
    }
    

    protected void handleSendException(WriteMessage w, IOException x) {
        ReceivePortIdentifier[] ports = null;
        synchronized (this) {
            ports = receivers.keySet()
                            .toArray(new ReceivePortIdentifier[0]);
        }

        if (x instanceof SplitterException) {
            SplitterException e = (SplitterException) x;

            Exception[] exceptions = e.getExceptions();
            java.io.OutputStream[] streams = e.getStreams();

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
            bufferedStream.close();
        } catch (Throwable e) {
            // ignored
        }

        out = null;
    }

}
