package dk.ku.di.dms.vms.modb.common.utils;

import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;

import java.nio.ByteBuffer;
import java.util.List;

import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.BATCH_OF_EVENTS;

public final class BatchUtils {

    public static int assembleBatchPayload(int remaining, List<TransactionEvent.PayloadRaw> events, ByteBuffer writeBuffer){
        int remainingBytes = writeBuffer.remaining();

        // no need. the client must make sure to deliver a clean buffer
        // writeBuffer.clear();
        writeBuffer.put(BATCH_OF_EVENTS);
        writeBuffer.position(5);

        // batch them all in the buffer,
        // until buffer capacity is reached or elements are all sent
        int count = 0;
        remainingBytes = remainingBytes - 1 - Integer.BYTES;
        int idx = events.size() - remaining;

        while(idx < events.size() && remainingBytes > events.get(idx).totalSize()){
            TransactionEvent.write( writeBuffer, events.get(idx) );
            remainingBytes = remainingBytes - events.get(idx).totalSize();
            idx++;
            count++;
        }

        writeBuffer.mark();
        writeBuffer.putInt(1, count);
        writeBuffer.reset();

        return remaining - count;
    }

}
