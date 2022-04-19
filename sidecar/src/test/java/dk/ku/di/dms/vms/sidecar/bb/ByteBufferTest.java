package dk.ku.di.dms.vms.sidecar.bb;

import jdk.incubator.foreign.MemoryAddress;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;

/**
 * Might be useful for dealing with bytebuffer objects: https://www.py4j.org/
 */
public class ByteBufferTest {

    @Test
    public void testReadWriteEvents(){




    }


    @Test
    public void testAllocation(){

//        ByteArrayInputStream bais = new ByteArrayInputStream()

//        ByteBuffer.allocate();

        Cleaner cleaner = Cleaner.create();

        try(ResourceScope scope = ResourceScope.newSharedScope(cleaner)) {

            MemorySegment segment = MemorySegment.allocateNative(100, scope);

            MemoryAddress memoryAddress = segment.address();

            // long address = memoryAddress.;

        }

    }

}
