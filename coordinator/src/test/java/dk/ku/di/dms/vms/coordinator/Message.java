package dk.ku.di.dms.vms.coordinator;

public class Message {

    public MessageType type;

    public DbmsDaemonThread dbmsDaemonThread;

    public ServerThread serverThread;

    public long timestamp;

    public long offset;

    public Message(MessageType type) {
        this.type = type;
    }

}
