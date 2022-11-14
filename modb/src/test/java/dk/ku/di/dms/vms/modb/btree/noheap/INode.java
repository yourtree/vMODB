package dk.ku.di.dms.vms.modb.btree.noheap;

import dk.ku.di.dms.vms.modb.definition.key.IKey;

public interface INode {

    int lastKey();

    Long insert(IKey key, long srcAddress);

}
