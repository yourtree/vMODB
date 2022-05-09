package dk.ku.di.dms.vms.coordinator;

public class Message {

    public MessageType type;

    //public OriginClass originType; this is inferred by the message type

    public DbmsDaemonThread dbmsDaemonThread;

    public ServerThread serverThread;

    public Object payload;

    public long timestamp;

    public Message(MessageType type, Object payload) {
        this.type = type;
        this.payload = payload;
    }

}
