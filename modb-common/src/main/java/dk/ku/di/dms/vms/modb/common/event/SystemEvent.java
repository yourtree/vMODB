package dk.ku.di.dms.vms.modb.common.event;

/**
 * This is the base class for representing system-related
 * information that must be exchanged between sdk and sidecar
 *
 * Aborts (throwing and receiving)
 * Handshake (after sending the data and event schemas)
 *
 */
public class SystemEvent implements IVmsEvent {

    public int op; // 0 = ERR 1 = OK

    /**
     * Depending on the type of system event, this can mean different things
     * For abort, this is the tid
     *
     */
    public int identifier;

    public String message; // message ERR

    public SystemEvent(int op, String message) {
        this.op = op;
        this.message = message;
    }
}