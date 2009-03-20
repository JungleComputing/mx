
package ibis.ipl.impl.mx;

interface MxProtocol {
    static final byte NEW_RECEIVER = 1;

    static final byte NEW_MESSAGE = 2;

    static final byte NEW_CONNECTION = 12;

    static final byte EXISTING_CONNECTION = 13;

    static final byte QUIT_IBIS = 14;

    static final byte REPLY = 127;
    
    static final int BUFSIZE = 128 * 1024;
}
