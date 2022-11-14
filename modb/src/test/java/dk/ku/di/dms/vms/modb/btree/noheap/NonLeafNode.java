package dk.ku.di.dms.vms.modb.btree.noheap;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.storage.record.AppendOnlyBuffer;
import dk.ku.di.dms.vms.modb.storage.record.OrderedRecordBuffer;

/**
 * how to represent the leaf nodes? with {@link OrderedRecordBuffer}
 * how to represent the internal (and parent) nodes? a proper data structure:
 * every key node has address left, right
 * sequentially put key by key
 * { left node, key value, right node }
 * { LONG, INT, LONG }
 *
 */
public class NonLeafNode {

    public static final byte identifier = 0;

    public static final int nonLeafEntrySize = Integer.BYTES + (2 * Long.BYTES);

    public static int branchingFactor = (MemoryUtils.UNSAFE.pageSize() / nonLeafEntrySize) - 1;

    public AppendOnlyBuffer buffer;

    private long first;
    private long last;
    private long half;

    public int nKeys;

    public INode[] children;

    public static final int deltaPrevious = Integer.BYTES;
    public static final int deltaNext = Integer.BYTES + Long.BYTES;

    public boolean parent;

    private NonLeafNode(){}

    public static NonLeafNode parent(){

        NonLeafNode parent = new NonLeafNode();

        // parent.leafSize = leafSize; // spread over internal nodes
        // parent.branchingFactor = branchingFactor;

        MemoryRefNode memoryRefNode = MemoryManager.getTemporaryDirectMemory( MemoryUtils.UNSAFE.pageSize() );
        parent.buffer = new AppendOnlyBuffer( memoryRefNode.address, memoryRefNode.bytes );
        parent.parent = true;
        parent.nKeys = 0;

        parent.children = new INode[branchingFactor + 1];

        // allocate another append only buffer and then an ordered record buffer

        // parent.children[0] = OrderedRecordBuffer

        return parent;

    }


    public Long insert(IKey key, long srcAddress) {

        // find the node

        long currAddr = this.buffer.address();

        int i;
        for(i = 0; i <= nKeys; i++){

            int currKey = MemoryUtils.UNSAFE.getInt(currAddr);

            if(currKey > key.hashCode())
                break;

            i++;
            currAddr = MemoryUtils.UNSAFE.getLong(currAddr + deltaNext);
        }

        Long newAddress = children[i].insert(key, srcAddress);

        if(newAddress != null){

        }

        return null;

    }


}
