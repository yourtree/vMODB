package dk.ku.di.dms.vms.modb.query.execution.operators.scan;

import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;

import java.util.Iterator;
import java.util.List;

public class FullScanWithProjection extends AbstractScan {

    public FullScanWithProjection(IMultiVersionIndex index,
                                  int[] projectionColumns,
                                  int entrySize) {
        super(entrySize, index, projectionColumns);
    }

//    public MemoryRefNode run(FilterContext filterContext){
//        Iterator<IKey> iterator = index.iterator();
//        while(iterator.hasNext()){
//            if(index.checkCondition(iterator, filterContext)){
//                append(iterator, projectionColumns);
//            }
//            iterator.next();
//        }
//        return memoryRefNode;
//    }

    public List<Object[]> runAsEmbedded(){
        // TODO return objects
        Iterator<Object[]> iterator = this.index.iterator();
        while(iterator.hasNext()){
            iterator.next();
        }
        // return this.memoryRefNode;
        return null;
    }

    @Override
    public boolean isFullScan() {
        return true;
    }

    @Override
    public FullScanWithProjection asFullScan() {
        return this;
    }

}
