package dk.ku.di.dms.vms.modb.btree.noheap;

import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.storage.record.OrderedRecordBuffer;

public class LeafNode {

    public static final byte identifier = 1;

    public static final int leafEntrySize = OrderedRecordBuffer.entrySize;
//
//    public static final int numberRecordsLeafNode =
//            ( MemoryUtils.UNSAFE.pageSize() + (2 * Long.BYTES) )

    public static int branchingFactor = (MemoryUtils.UNSAFE.pageSize() / leafEntrySize) - 1;



}
