package dk.ku.di.dms.vms.modb.index.offheap;

import dk.ku.di.dms.vms.modb.schema.Schema;
import jdk.incubator.foreign.*;

import java.nio.ByteBuffer;

/**
 *
 * https://docs.oracle.com/en/java/javase/16/docs/api/jdk.incubator.foreign/jdk/incubator/foreign/MemoryLayout.html
 *
 */
public class MemoryMappingFactory {

    public static GroupLayout buildGroupLayout(Schema schema){
        return null;
    }

    /**
     * ByteBuffer to maintain interoperability
     * in case the new API cannot be used
     * @param size Number of expected records
     * @param struct The schema as {@link GroupLayout}
     * @return
     */
    public static MemorySegment load(long size, GroupLayout struct ) {

        SequenceLayout SEQUENCE_LAYOUT = MemoryLayout.sequenceLayout(struct);

        MemorySegment segment = MemorySegment.allocateNative(
                SEQUENCE_LAYOUT.byteSize() * size,
                //SEQUENCE_LAYOUT.byteAlignment(),
                ResourceScope.globalScope());

        // segment.get(ValueLayout.OfBoolean.JAVA_BOOLEAN, 1209 );

        return segment;
    }

    public static void getByAddress( MemorySegment segment, long address ){

        MemoryAddress memoryAddress = segment.get(ValueLayout.OfAddress.ADDRESS, address);

//         memoryAddress.s

    }

    /*
        GroupLayout struct = MemoryLayout.structLayout(

        );

        SequenceLayout SEQUENCE_LAYOUT = MemoryLayout.sequenceLayout(1024, struct);
        SequenceLayout TEST = MemoryLayout.sequenceLayout( struct);


        MemorySegment segment = MemorySegment.allocateNative(SEQUENCE_LAYOUT, ResourceScope.globalScope());

        segment.spliterator()

        // can be used in queries without indexes
        int sum = segment.elements(struct).parallel()

                                                .mapToInt(s -> s.get(ValueLayout.JAVA_INT, 0))
                                                .sum();
//
//        MemorySegment segment = MemorySegment.allocateNative(struct, ResourceScope.globalScope());
//
//        SequenceLayout SEQUENCE_LAYOUT = MemoryLayout.sequenceLayout(1024, ValueLayout.JAVA_INT);

        return MemorySegment.allocateNative( size, ResourceScope.globalScope() );
    }
     */

}
