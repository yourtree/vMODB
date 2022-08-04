package dk.ku.di.dms.vms.modb.persistence;

import jdk.incubator.foreign.*;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

public class PersistenceTest {

    // private static final 10737418240
    private static final long ONE_GB = 1073741824;
    private static final long TWO_GB = Integer.MAX_VALUE;
    private static final long TEN_GB = ONE_GB * 10;

    @Test
    public void testMemoryMapping(){
        // must store this segment in the catalog
        // MemorySegment segment = MemorySegment.allocateNative( ONE_GB * 10L, ResourceScope.globalScope() );

    }

    @Test
    public void testFileMapping() throws IOException {

        String userHome = System.getProperty("user.home");

        String filePath = userHome + "/" + "testFile";

        File file = new File(filePath);
        boolean res = false;
        if(!file.exists()){
            res = file.createNewFile();
        }

        assert res;

        long divFactor = TWO_GB - 8;

        // every byte buffer "loses" 8 bytes per mapping

        int numberBuckets = (int) (TEN_GB / divFactor);

        long lostBytes = numberBuckets * 8;

        MemorySegment segment = MemorySegment.mapFile(
                file.toPath(),
                0,
                TEN_GB - lostBytes,
                FileChannel.MapMode.READ_WRITE,
                ResourceScope.newSharedScope());

        long offsetInit = 0;
        // long offsetEnd = divFactor - 1;

        ByteBuffer mappedBuffer;

        Map<Integer, ByteBuffer> buffers = new HashMap<>( numberBuckets);
        for(int i = 0; i < numberBuckets; i++){

//            long bufSize = offsetEnd-offsetInit;
//            System.out.println("Size of the buffer "+i+" :"+bufSize);

            MemorySegment seg0 = segment.asSlice(offsetInit, divFactor);
            mappedBuffer = seg0.asByteBuffer();

            buffers.put(i, mappedBuffer);

            offsetInit += divFactor;
            offsetInit++;
        }

        mappedBuffer = buffers.get(0);

        ByteBuffer appBuffer = ByteBuffer.allocate(Integer.BYTES);

        appBuffer.putInt(1);

        // do it before it gets read by mapped buffer
        appBuffer.clear();

        mappedBuffer.put( appBuffer );

        segment.force();

        mappedBuffer.clear();

        // does the segment increases offset after calling
        buffers.get(1).putInt(2);

        int testInt = mappedBuffer.getInt();

        System.out.println(testInt);

        assert(testInt != 2);

    }

    // create here a test.. a 4 gb segment is created and then a slice of 2gb is created.
    // the slice is transformed into a bytebuffer. does this slice starts at 0?

}
