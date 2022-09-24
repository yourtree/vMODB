package dk.ku.di.dms.vms.modb.service;


import org.junit.Test;

import java.lang.foreign.MemoryAddress;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.MemorySession;
import java.lang.ref.Cleaner;

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

        try(MemorySession scope = MemorySession.openShared(cleaner)) {

            MemorySegment segment = MemorySegment.allocateNative(100, scope);

            MemoryAddress memoryAddress = segment.address();

            // long address = memoryAddress.;

        }

    }

}
