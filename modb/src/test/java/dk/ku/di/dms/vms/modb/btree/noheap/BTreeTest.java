package dk.ku.di.dms.vms.modb.btree.noheap;

import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
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

        parent.insert(KeyUtils.buildKey(1), 1L);
        parent.insert(KeyUtils.buildKey(2), 2L);
        parent.insert(KeyUtils.buildKey(3), 3L);
        parent.insert(KeyUtils.buildKey(4), 4L);
        parent.insert(KeyUtils.buildKey(5), 5L);
//        parent.insert(KeyUtils.buildKey(6), 6L);
//        parent.insert(KeyUtils.buildKey(7), 7L);

        IRecordIterator<Long> iterator = parent.iterator();

        while(iterator.hasElement()){
            System.out.println(MemoryUtils.UNSAFE.getInt( iterator.get() + 1 + Long.BYTES + Integer.BYTES ));
            iterator.next();
        }

    }

}
