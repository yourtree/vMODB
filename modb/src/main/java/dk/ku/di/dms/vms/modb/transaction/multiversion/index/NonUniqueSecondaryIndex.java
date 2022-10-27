package dk.ku.di.dms.vms.modb.transaction.multiversion.index;

import dk.ku.di.dms.vms.modb.common.transaction.TransactionMetadata;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.IIndexKey;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadOnlyIndex;
import dk.ku.di.dms.vms.modb.index.non_unique.NonUniqueHashIndex;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class NonUniqueSecondaryIndex implements ReadOnlyIndex<IKey> {

    // pointer to primary index
    // necessary because of transaction, concurrency control
    private final PrimaryIndex primaryIndex;

    // a non-unique hash index
    private final NonUniqueHashIndex underlyingIndex;

    // key: formed by secondary indexed columns
    // value: the corresponding pks
    private final Map<IKey, Set<IKey>> writesCache;

    public NonUniqueSecondaryIndex(PrimaryIndex primaryIndex, NonUniqueHashIndex underlyingIndex) {
        this.primaryIndex = primaryIndex;
        this.underlyingIndex = underlyingIndex;
        this.writesCache = new ConcurrentHashMap<>();
    }

    @Override
    public IIndexKey key() {
        return this.underlyingIndex.key();
    }

    @Override
    public Schema schema() {
        return this.primaryIndex.schema();
    }

    @Override
    public int[] columns() {
        return this.underlyingIndex.columns();
    }

    @Override
    public boolean containsColumn(int columnPos) {
        return this.underlyingIndex.containsColumn(columnPos);
    }

    @Override
    public IndexTypeEnum getType() {
        return this.underlyingIndex.getType();
    }

    @Override
    public int size() {
        return this.underlyingIndex.size() + this.writesCache.size();
    }

    @Override
    public boolean exists(IKey key) {


        if(TransactionMetadata.TRANSACTION_CONTEXT.get().readOnly) {

        }

        return false;
    }

    @Override
    public IRecordIterator<IKey> iterator() {
        return null;
    }

    /**
     * The semantics of this method:
     * The bucket must have at least one record
     * Writers must read their writes.
     */
//    @Override
//    public boolean exists(IKey key) {
//
//        // find record in secondary index
//        if(this.writesCache.get(key) == null){
//            return this.underlyingSecondaryIndex.exists(key);
//        }
//
//        return true;
//
//        // retrieve PK from record
//
//        // make sure record exists in PK
//
//        // if not, delete from sec idx and return false
//
//        // if so, return yes
//
//    }

    /**
     * Called by the primary key index
     * In this method, the secondary key is formed
     * and then cached for later retrieval.
     * A secondary key point to several primary keys in the primary index
     * @param primaryKey may have many secIdxKey associated
     */
    public void appendDelta(IKey primaryKey, Object[] record){
        IKey secIdxKey = KeyUtils.buildRecordKey( this.underlyingIndex.columns(), record );
        Set<IKey> pkSet = this.writesCache
                .computeIfAbsent(secIdxKey, k -> ConcurrentHashMap.newKeySet());
        pkSet.add(primaryKey);
    }

    /**
     * @param record the record. must extract the values from columns
     * @return whether the set of keys has been deleted
     */
    public boolean delete(Object[] record) {

        IKey secIdxKey = KeyUtils.buildRecordKey( this.underlyingIndex.columns(), record );
        this.writesCache.remove(secIdxKey);

        // also delete from underlying?
        this.underlyingIndex.delete(secIdxKey);

        return true;

    }

}
