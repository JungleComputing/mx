package ibis.ipl.impl.mx;

import ibis.ipl.PortType;
import ibis.ipl.SendPortDisconnectUpcall;
import ibis.ipl.impl.Ibis;
import ibis.ipl.impl.SendPort;
import ibis.ipl.impl.SendPortIdentifier;

import java.io.IOException;
import java.util.Properties;

abstract class MxSendPort extends SendPort implements MxProtocol {
	
	MxSendPort(Ibis ibis, PortType type, String name,
            SendPortDisconnectUpcall cU, Properties props) throws IOException {
        super(ibis, type, name, cU, props);
    }

    SendPortIdentifier getIdent() {
        return ident;
    }

}
