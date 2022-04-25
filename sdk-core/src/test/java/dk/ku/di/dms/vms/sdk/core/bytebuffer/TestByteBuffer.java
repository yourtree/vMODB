package dk.ku.di.dms.vms.sdk.core.bytebuffer;

import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;
import dk.ku.di.dms.vms.sdk.core.example.EventExample;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class TestByteBuffer {

    @Test
    public void testByteBuffer(){


        try {

            File file = new File("/etc/vms-events");
            RandomAccessFile rafEvent = new RandomAccessFile(file, "rw");
            rafEvent.setLength( 1024 );
            FileChannel eventChannel = rafEvent.getChannel();
            ByteBuffer eventStoreByteBuffer = eventChannel.map( FileChannel.MapMode.READ_WRITE, 0, 1024 );

            // payload packet
            TransactionalEvent transactionalEvent = new TransactionalEvent( 1, "in", new EventExample(1) );


        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
