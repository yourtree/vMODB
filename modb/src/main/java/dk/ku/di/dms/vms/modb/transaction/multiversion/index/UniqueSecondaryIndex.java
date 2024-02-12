package dk.ku.di.dms.vms.modb.transaction.multiversion.index;

import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionId;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.transaction.internal.SingleWriterMultipleReadersFIFO;
import dk.ku.di.dms.vms.modb.transaction.multiversion.TransactionWrite;
import dk.ku.di.dms.vms.modb.transaction.multiversion.WriteType;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * The same key from PK is used to find records in this index
 * but only a portion of the data from the primary index is
 * found here. The criteria is given via annotating
 * {@link dk.ku.di.dms.vms.modb.api.annotations.VmsPartialIndex}
 * in a column belonging to a {@link VmsTable}.
 */
public final class UniqueSecondaryIndex implements IMultiVersionIndex {

    private final ThreadLocal<Map<IKey, WriteType>> KEY_WRITES = ThreadLocal.withInitial(HashMap::new);

    private final PrimaryIndex primaryIndex;

    private final Set<IKey> keyMap;

    public UniqueSecondaryIndex(PrimaryIndex primaryIndex) {
        this.primaryIndex = primaryIndex;
        this.keyMap = ConcurrentHashMap.newKeySet();
    }

    @Override
    public void undoTransactionWrites() {
        Map<IKey, WriteType> writesOfTid = KEY_WRITES.get();
        // nothing to do
        writesOfTid.clear();
    }

    @Override
    public void installWrites() {
        Map<IKey, WriteType> writesOfTid = KEY_WRITES.get();
        if(writesOfTid == null) return;
        for(var entry : writesOfTid.entrySet()){
            switch (entry.getValue()){
                case INSERT -> this.keyMap.add(entry.getKey());
                case DELETE -> this.keyMap.remove(entry.getKey());
            }
        }
        writesOfTid.clear();
    }

    @Override
    public boolean insert(IKey key, Object[] record) {
        KEY_WRITES.get().put(key, WriteType.INSERT);
        return true;
    }

    @Override
    public boolean update(IKey key, Object[] record) {
        // we assume it is already there
        // KEY_WRITES.get().put(key, WriteType.UPDATE);
        return true;
    }

    @Override
    public boolean remove(IKey key) {
        this.KEY_WRITES.get().put(key, WriteType.DELETE);
        return true;
    }

    @Override
    public Object[] lookupByKey(IKey key){
        return null; // should never call it. this index does not have the record
    }

    @Override
    public Iterator<Object[]> iterator() {
        return new MultiVersionIterator(this.primaryIndex, KEY_WRITES.get(), this.keyMap.iterator());
    }

    private static class MultiVersionIterator implements Iterator<Object[]> {

        private final PrimaryIndex primaryIndex;
        private final Iterator<IKey> iterator;

        private final Map<IKey, WriteType> writeSet;
        private Iterator<Map.Entry<IKey, WriteType>> currentTidIterator;

        SingleWriterMultipleReadersFIFO.Entry<TransactionId, TransactionWrite> entry;
        IKey key;

        Supplier<Boolean> iteratorSupplier;

        public MultiVersionIterator(PrimaryIndex primaryIndex, Map<IKey, WriteType> writeSet, Iterator<IKey> iterator){
            this.primaryIndex = primaryIndex;
            this.writeSet = writeSet;
            this.iterator = iterator;
            this.iteratorSupplier = iterator::hasNext;
        }

        @Override
        public boolean hasNext() {
            return iteratorSupplier.get();
        }

        @Override
        public Object[] next() {
            if(iterator.hasNext()) {
                key = iterator.next();
                // remove if it contains the key
                writeSet.remove(key);
                if(!iterator.hasNext()){
                    this.currentTidIterator = writeSet.entrySet().iterator();
                    this.iteratorSupplier = currentTidIterator::hasNext;
                }
            } else {
                var entryCurr = currentTidIterator.next();
                if(entryCurr.getValue() == WriteType.DELETE)
                    return null;
                key = entryCurr.getKey();
            }
            entry = primaryIndex.getFloorEntry(key);
            if (entry != null)
                return entry.val().record;
            return null;
        }

    }

    @Override
    public Iterator<Object[]> iterator(IKey[] keys) {
        return new KeyMultiVersionIterator(keys);
    }

    private class KeyMultiVersionIterator implements Iterator<Object[]> {
        private final IKey[] keys;
        private final Map<IKey, WriteType> writeSet;
        private int idx = 0;
        public KeyMultiVersionIterator(IKey[] keys){
            this.keys = keys;
            this.writeSet = KEY_WRITES.get();
        }

        @Override
        public boolean hasNext() {
            if(idx == keys.length) return false;
            if((writeSet.containsKey(keys[idx]) && writeSet.get(keys[idx]) != WriteType.DELETE) || keyMap.contains(keys[idx])) {
                idx++;
                return true;
            }
            idx++;
            return false;
        }

        @Override
        public Object[] next() {
            var obj = primaryIndex.getFloorEntry(keys[idx]);
            return obj.val().record;
        }

    }

    @Override
    public int[] indexColumns() {
        return this.primaryIndex.indexColumns();
    }

    @Override
    public boolean containsColumn(int columnPos) {
        return this.primaryIndex.containsColumn(columnPos);
    }

}
