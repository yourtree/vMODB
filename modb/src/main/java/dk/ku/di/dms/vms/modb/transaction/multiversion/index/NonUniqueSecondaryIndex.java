package dk.ku.di.dms.vms.modb.transaction.multiversion.index;

import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionId;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.transaction.internal.SingleWriterMultipleReadersFIFO;
import dk.ku.di.dms.vms.modb.transaction.multiversion.TransactionWrite;
import dk.ku.di.dms.vms.modb.transaction.multiversion.WriteType;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Wrapper of a non unique index for multi versioning concurrency control
 */
public final class NonUniqueSecondaryIndex implements IMultiVersionIndex {

    private final ThreadLocal<Map<IKey, Tuple<Object[], WriteType>>> KEY_WRITES = ThreadLocal.withInitial(HashMap::new);

    // pointer to primary index
    // necessary because of concurrency control
    // secondary index point to records held in primary index
    private final PrimaryIndex primaryIndex;

    // a non-unique hash index
    private final ReadWriteIndex<IKey> underlyingIndex;

    // key: formed by secondary indexed columns
    // value: the corresponding pks
    private final Map<IKey, Set<IKey>> keyMap;

    public NonUniqueSecondaryIndex(PrimaryIndex primaryIndex, ReadWriteIndex<IKey> underlyingIndex) {
        this.primaryIndex = primaryIndex;
        this.underlyingIndex = underlyingIndex;
        this.keyMap = new ConcurrentHashMap<>();
    }

    public ReadWriteIndex<IKey> getUnderlyingIndex(){
        return this.underlyingIndex;
    }

    /**
     * Called by the primary key index
     * In this method, the secondary key is formed
     * and then cached for later retrieval.
     * A secondary key point to several primary keys in the primary index
     * @param primaryKey may have many secIdxKey associated
     */
    @Override
    public boolean insert(IKey primaryKey, Object[] record){
        IKey secKey = KeyUtils.buildRecordKey( this.underlyingIndex.columns(), record );
        Set<IKey> set = this.keyMap.computeIfAbsent(secKey, (x)-> new HashSet<>());
        if(!set.contains(primaryKey)) {
            KEY_WRITES.get().put(primaryKey, new Tuple<>(record, WriteType.INSERT));
            set.add(primaryKey);
        }
        return true;
    }

    @Override
    public void undoTransactionWrites(){
        var writes = KEY_WRITES.get();
        for(var entry : writes.entrySet()){
            IKey secKey = KeyUtils.buildRecordKey( this.underlyingIndex.columns(), entry.getValue().t1() );
            Set<IKey> set = this.keyMap.get(secKey);
            set.remove(entry.getKey());
        }
    }

    @Override
    public void installWrites() {
        // just remove the delete TODO separate INSERT and DELETE into different maps
        var deletes = KEY_WRITES.get().entrySet().stream().filter(p->p.getValue().t2()==WriteType.DELETE).toList();
        for(var entry : deletes){
            IKey secKey = KeyUtils.buildRecordKey( this.underlyingIndex.columns(), entry.getValue().t1() );
            Set<IKey> set = this.keyMap.get(secKey);
            set.remove(entry.getKey());
        }
    }

    @Override
    public boolean update(IKey key, Object[] record) {
        // KEY_WRITES.get().put(key, new Tuple<>(record, WriteType.UPDATE));
        // key already there
        return true;
    }

    @Override
    public boolean remove(IKey key) {
        // how to know the sec idx if we don't have the record?
        return false;
    }

    public boolean remove(IKey key, Object[] record){
        KEY_WRITES.get().put(key, new Tuple<>(record, WriteType.DELETE));
        return true;
    }

    @Override
    public Object[] lookupByKey(IKey key) {
        throw new RuntimeException("Not supported");
    }

    @Override
    public Iterator<Object[]> iterator() {
        return new MultiVersionIterator();
    }

    private static class MultiVersionIterator implements Iterator<Object[]> {

        public MultiVersionIterator(){

        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Object[] next() {
            return new Object[0];
        }
    }

    @Override
    public Iterator<Object[]> iterator(IKey[] keys) {
        return new MultiKeyMultiVersionIterator(this.primaryIndex, keys, this.keyMap);
    }

    private static class MultiKeyMultiVersionIterator implements Iterator<Object[]> {

        private final Map<IKey, Set<IKey>> keyMap;
        private final PrimaryIndex primaryIndex;
        private Iterator<IKey> currentIterator;

        private final IKey[] keys;

        private int idx;

        public MultiKeyMultiVersionIterator(PrimaryIndex primaryIndex, IKey[] keys, Map<IKey, Set<IKey>> keyMap){
            this.primaryIndex = primaryIndex;
            this.idx = 0;
            this.keys = keys;
            this.currentIterator = keyMap.get(keys[this.idx]).iterator();
            this.keyMap = keyMap;
        }

        @Override
        public boolean hasNext() {
            return currentIterator.hasNext() || idx < keys.length - 1;
        }

        @Override
        public Object[] next() {
            if(!currentIterator.hasNext()){
                idx++;
                currentIterator = keyMap.get(keys[this.idx]).iterator();
            }
            IKey key = currentIterator.next();
            SingleWriterMultipleReadersFIFO.Entry<Long, TransactionWrite> entry = primaryIndex.getFloorEntry(key);
            if (entry != null)
                return entry.val().record;
            return null;
        }

    }

    @Override
    public int[] indexColumns() {
        return this.underlyingIndex.columns();
    }

    @Override
    public boolean containsColumn(int columnPos) {
        return this.underlyingIndex.containsColumn(columnPos);
    }

}
