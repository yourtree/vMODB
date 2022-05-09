package dk.ku.di.dms.vms.coordinator;

public enum MessageType {

    EVENT,
    EVENT_ACK,
    EVENT_NACK,

    CHECKPOINT,
    CHECKPOINT_COMPLETING, // tells the server to wait a bit
    CHECKPOINT_ACK,

    CHECKPOINT_NACK, // in what situations do we have a nack?

    CHECKPOINT_INFO, // checkpoint info request sent by server to a (input) dbms

    SUB_BATCH, // only from non-leader serves



    // TODO leader election messages

}
