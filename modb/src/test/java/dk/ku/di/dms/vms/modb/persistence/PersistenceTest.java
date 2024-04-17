package dk.ku.di.dms.vms.modb.persistence;

import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class PersistenceTest {

    private static jdk.internal.misc.Unsafe unsafe;

    @BeforeClass
    public static void setUp(){
        unsafe = MemoryUtils.UNSAFE;
        assert unsafe != null;
    }

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

        MemorySegment segment;
        try (Arena arena = Arena.ofShared()) {
            segment = arena.allocate(TEN_GB - lostBytes);
        }

        long nextOffset = 0;
        // long offsetEnd = divFactor - 1;

        ByteBuffer mappedBuffer;

        long[] initOffsetPerBuffer = new long[numberBuckets];

        Map<Integer, ByteBuffer> buffers = new HashMap<>( numberBuckets);
        for(int i = 0; i < numberBuckets; i++){

            initOffsetPerBuffer[i] = nextOffset;
//            long bufSize = offsetEnd-offsetInit;
//            System.out.println("Size of the buffer "+i+" :"+bufSize);

            MemorySegment seg0 = segment.asSlice(nextOffset, divFactor);
            mappedBuffer = seg0.asByteBuffer();

            buffers.put(i, mappedBuffer);

            nextOffset += divFactor;
            nextOffset++;
        }

        mappedBuffer = buffers.get(0);

        ByteBuffer appBuffer = ByteBuffer.allocate(Integer.BYTES);
        appBuffer.putInt(1);
        appBuffer.clear();

        // writing an integer value to mapped buffer
        mappedBuffer.put( appBuffer );

        // flush
        segment.force();

        // reset position
        mappedBuffer.clear();

        assert(mappedBuffer.getInt() == appBuffer.getInt());

        // next check is regarding the last buffer
        long offsetToTest = initOffsetPerBuffer[2];

        mappedBuffer.position(0);
        mappedBuffer.putInt(10);

        // https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/core/memory/MemoryUtils.java
        unsafe.copyMemory( segment.address(), segment.address() + offsetToTest, Integer.BYTES );

        assert (buffers.get(2).getInt() == 10);

    }

}
