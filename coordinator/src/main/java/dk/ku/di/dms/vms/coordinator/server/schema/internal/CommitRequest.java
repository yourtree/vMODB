package dk.ku.di.dms.vms.coordinator.server.schema.internal;

import java.nio.ByteBuffer;

import static dk.ku.di.dms.vms.coordinator.server.Constants.COMMIT_REQUEST;

/**
 * A batch-commit request payload
 */
public final class CommitRequest {

    // commit  vms last tid
    // type  | offset       |
    private static final int headerSize = Byte.BYTES + Long.BYTES;

    // send the last tid (corresponding to the vms) and batch id
    public static void write(ByteBuffer buffer, long tid, long batch){

        buffer.put( COMMIT_REQUEST );
        buffer.putLong( tid );
        buffer.putLong( batch );

    }

    public static CommitRequestPayload read(ByteBuffer buffer){
        long tid = buffer.getLong();
        long batch = buffer.getLong();
        return new CommitRequestPayload(tid,batch);
    }

    public record CommitRequestPayload (
            long tid, long batch
    ){}

}
