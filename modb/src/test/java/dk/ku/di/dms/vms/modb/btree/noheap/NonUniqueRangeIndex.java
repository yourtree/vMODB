package dk.ku.di.dms.vms.modb.btree.noheap;

import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.IIndexKey;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;
import dk.ku.di.dms.vms.modb.storage.record.OrderedRecordBuffer;

import java.util.Map;


public class NonUniqueRangeIndex implements ReadWriteIndex<IKey> {


    NonLeafNode parent;

    Map<Long, NonLeafNode> internalNodes;

    Map<Long, LeafNode> leafNodes;


    @Override
    public IIndexKey key() {
        return null;
    }

    @Override
    public Schema schema() {
        return null;
    }

    @Override
    public int[] columns() {
        return new int[0];
    }

    @Override
    public boolean containsColumn(int columnPos) {
        return false;
    }

    @Override
    public IndexTypeEnum getType() {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean exists(IKey key) {
        return false;
    }

    @Override
    public IRecordIterator<IKey> iterator() {
        return null;
    }

    @Override
    public void insert(IKey key, long srcAddress) {

        //
        parent.insert(key, srcAddress);

    }

    @Override
    public void update(IKey key, long srcAddress) {

    }

    @Override
    public void delete(IKey key) {

    }
}
