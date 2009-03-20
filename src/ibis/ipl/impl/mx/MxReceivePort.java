
package ibis.ipl.impl.mx;

import ibis.ipl.MessageUpcall;
import ibis.ipl.PortType;
import ibis.ipl.ReceivePortConnectUpcall;
import ibis.ipl.impl.Ibis;
import ibis.ipl.impl.ReceivePort;
import ibis.ipl.impl.SendPortIdentifier;

import java.io.IOException;
import java.util.Properties;

import mxio.ConnectionRequest;

/* based on the ReceivePort of TCPIbis */

abstract class MxReceivePort extends ReceivePort implements MxProtocol {

    MxReceivePort(Ibis ibis, PortType type, String name, MessageUpcall upcall,
            ReceivePortConnectUpcall connUpcall, Properties props) throws IOException {
        super(ibis, type, name, upcall, connUpcall, props);
       
    }
    
    abstract void accept(ConnectionRequest req, SendPortIdentifier origin, PortType sp);
  
}
