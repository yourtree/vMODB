package dk.ku.di.dms.vms.modb.persistence;

import jdk.incubator.foreign.*;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class PersistenceTest {

    // private static final 10737418240
    private static final int ONE_GB = 1073741824;

    @Test
    public void testMemoryMapping(){

        // must store this segment in the catalog
        MemorySegment segment = MemorySegment.allocateNative( ONE_GB * 10L, ResourceScope.globalScope() );

        // segment.unload();
    }

    @Test
    public void testFileMapping() throws IOException {

        String userHome = System.getProperty("user.home");

        String filePath = userHome + "/" + "testFile";

        File file = new File(filePath);
        if(!file.exists()){
            file.createNewFile();
        }

        MemorySegment segment = MemorySegment.mapFile(
                file.toPath(),
                0,
                ONE_GB,
                FileChannel.MapMode.READ_WRITE,
                ResourceScope.globalScope());

        ByteBuffer mappedBuffer = segment.asByteBuffer();

        ByteBuffer appBuffer = ByteBuffer.allocate(Integer.BYTES);

        appBuffer.putInt(1);

        // do it before it gets read by mapped buffer
        appBuffer.clear();

        mappedBuffer.put( appBuffer );

        segment.force();

        mappedBuffer.clear();



        int testInt = mappedBuffer.getInt();

        System.out.println(testInt);

    }

}
