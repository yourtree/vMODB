package dk.ku.di.dms.vms.modb.transaction.multiversion.index;

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
    private final NonUniqueHashIndex underlyingSecondaryIndex;

    // key: formed by secondary indexed columns
    private final Map<IKey, Set<IKey>> writesCache;

    public NonUniqueSecondaryIndex(PrimaryIndex primaryIndex, NonUniqueHashIndex underlyingSecondaryIndex) {
        this.primaryIndex = primaryIndex;
        this.underlyingSecondaryIndex = underlyingSecondaryIndex;
        this.writesCache = new ConcurrentHashMap<>();
    }

    @Override
    public IIndexKey key() {
        return this.underlyingSecondaryIndex.key();
    }

    @Override
    public Schema schema() {
        return this.primaryIndex.schema();
    }

    @Override
    public int[] columns() {
        return this.underlyingSecondaryIndex.columns();
    }

    @Override
    public boolean containsColumn(int columnPos) {
        return this.underlyingSecondaryIndex.containsColumn(columnPos);
    }

    @Override
    public IndexTypeEnum getType() {
        return this.underlyingSecondaryIndex.getType();
    }

    @Override
    public int size() {
        return this.underlyingSecondaryIndex.size();
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
    public IRecordIterator<IKey> iterator(IKey[] keys) {
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
     * In this method, the secondary key is formed and then cached
     * for later retrieval
     * @param primaryKey may have many secIdxKey associated
     */
    public void write(IKey primaryKey, Object[] record){
        IKey secIdxKey = KeyUtils.buildRecordKey( this.underlyingSecondaryIndex.columns(), record );
        Set<IKey> pkSet = this.writesCache.get( secIdxKey );
        if(pkSet == null){
            pkSet = ConcurrentHashMap.newKeySet();
        }
        pkSet.add(primaryKey);
        this.writesCache.put( secIdxKey, pkSet );
    }

    /**
     * A secondary key point to several primary keys in the primary index
     * @param key the secondary index key
     * @return the set of primary keys
     */
//    public Set<IKey> retrievePrimaryKeys(IKey key){
//        // merge keys
//        Set<IKey> setOfFreshWrites = this.writesCache.get(key);
//
//        // build list from iterator
//        IRecordIterator<Long> iterator = this.underlyingSecondaryIndex.iterator( key );
//        List<IKey> setOfOldKeys = new ArrayList<>(  );
//        while(iterator.hasNext()){
//            setOfOldKeys.add( iterator.get() );
//            iterator.next();
//        }
//
//        setOfFreshWrites.addAll( setOfOldKeys );
//        return setOfFreshWrites;
//    }
//
//    public List<Object[]> retrieveRecords(IKey key){
//
//    }

}
