package dk.ku.di.dms.vms.modb.btree.noheap;

import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.IntKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;
import dk.ku.di.dms.vms.modb.storage.iterator.non_unique.RecordBucketIterator;
import org.junit.Test;

public class BTreeTest {

    private static final int PAGE_SIZE_KB = 128;

    // several insertions and then walk through the entries
    @Test
    public void testExecutor(){

        NonLeafNode parent = NonLeafNode.parent( PAGE_SIZE_KB );

        parent.insert(IntKey.of(1), 1L);
        parent.insert(IntKey.of(2), 2L);
        parent.insert(IntKey.of(3), 3L);
        parent.insert(IntKey.of(4), 4L);
        parent.insert(IntKey.of(5), 5L);
        parent.insert(IntKey.of(6), 6L);
//        parent.insert(IntKey.of(7), 7L);

        IRecordIterator<Long> iterator = parent.iterator();

        while(iterator.hasElement()){
            System.out.println(MemoryUtils.UNSAFE.getInt( iterator.get() + 1 + Long.BYTES + Integer.BYTES ));
            iterator.next();
        }

        // FIXME insert 7 not working

    }

}
