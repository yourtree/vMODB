package dk.ku.di.dms.vms.modb.query.execution.operators.scan;

import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import dk.ku.di.dms.vms.modb.transaction.TransactionContext;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * On-flight scanning, filtering, and projection in a single operator.
 * The caller knows the output format.
 * But called does not know how many records are.
 * Header:
 * int - number of rows returned
 * This must be performed by a proper method at the end of the procedure
 * We can have a method called seal or close() in abstract operator
 * It will put the information on header.
 */
public final class IndexScanWithProjection extends AbstractScan {

    public IndexScanWithProjection(
                     IMultiVersionIndex index,
                     int[] projectionColumns,
                     int entrySize) {
        super(entrySize, index, projectionColumns);
    }

    @Override
    public boolean isIndexScan() {
        return true;
    }

    @Override
    public IndexScanWithProjection asIndexScan() {
        return this;
    }

    public List<Object[]> runAsEmbedded(TransactionContext txCtx){
        // TODO return objects
        Iterator<Object[]> iterator = this.index.iterator(txCtx);
        while(iterator.hasNext()){

            iterator.next();
        }
        // return this.memoryRefNode;
        return null;
    }

    public List<Object[]> runAsEmbedded(TransactionContext txCtx, IKey[] keys){
        List<Object[]> res = new ArrayList<>();
        Iterator<Object[]> iterator = this.index.iterator(txCtx, keys);
        while(iterator.hasNext()){
            res.add( iterator.next() );
        }
        return res;
    }

    public MemoryRefNode run(TransactionContext txCtx, FilterContext filterContext, IKey... keys) {
        // unifying in terms of iterator
        Iterator<Object[]> iterator = index.iterator(txCtx, keys);
        while(iterator.hasNext()){
            Object[] record = iterator.next();
            if(index.checkCondition(filterContext, record)){
                // this.append(iterator, this.projectionColumns);
            }

        }
        // return this.memoryRefNode;
        return null;
    }

//    public MemoryRefNode run(IKey... keys) {
//        Iterator<IKey> iterator = this.index.iterator(keys);
//        while(iterator.hasNext()){
//            this.append(iterator, this.projectionColumns);
//            iterator.next();
//        }
//        return this.memoryRefNode;
//    }

    // return entities directly

}