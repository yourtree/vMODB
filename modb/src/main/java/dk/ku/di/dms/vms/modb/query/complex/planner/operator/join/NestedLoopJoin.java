//package dk.ku.di.dms.vms.modb.query.complex.planner.operator.join;
//
//import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
//import dk.ku.di.dms.vms.modb.query.refac.FilterContext;
//import dk.ku.di.dms.vms.modb.definition.key.IKey;
//import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
//import dk.ku.di.dms.vms.modb.storage.memory.MemoryRefNode;
//import dk.ku.di.dms.vms.modb.storage.iterator.RecordIterator;
//
///**
// * Iterate over the PK
// */
//public class NestedLoopJoin extends AbstractJoin {
//
//    private final int[] outerColumns;
//    private final int[] innerColumns;
//
//    public NestedLoopJoin(int id, int entrySize,
//                          UniqueHashIndex outerIndex, int[] outerColumns,
//                          UniqueHashIndex innerIndex, int[] innerColumns,
//                          FilterContext filterOuter, FilterContext filterInner) {
//        super(id, entrySize, outerIndex, innerIndex, filterOuter, filterInner);
//        this.outerColumns = outerColumns;
//        this.innerColumns = innerColumns;
//    }
//
//    @Override
//    public JoinOperatorTypeEnum getType() {
//        return JoinOperatorTypeEnum.NESTED_LOOP;
//    }
//
//
//    public MemoryRefNode get() {
//
//        RecordIterator outerIterator = leftIndex.asUniqueHashIndex().iterator();
//        RecordIterator innerIterator;
//
//        long outerAddress;
//        long innerAddress;
//        boolean match;
//
//        while(outerIterator.hasNext()){
//
//            outerAddress = outerIterator.next();
//
//            if(checkCondition(outerAddress, leftFilter, leftIndex)){
//
//                // this key is not materialized in the outer, but in the inner
//                IKey outerProbeKey = KeyUtils.buildRecordKey( leftIndex.getTable().getSchema(), outerColumns, outerAddress );
//
//                // get the iterator
//                innerIterator = rightIndex.asUniqueHashIndex().iterator();
//
//                while(innerIterator.hasNext()){
//                    innerAddress = innerIterator.next();
//
//                    IKey innerProbeKey = KeyUtils.buildRecordKey( leftIndex.getTable().getSchema(), outerColumns, outerAddress );
//                    match = outerProbeKey.hashCode() == innerProbeKey.hashCode();
//
//                    if( match && checkCondition(innerAddress, rightFilter, rightIndex)) {
//
//                        append( outerAddress, innerAddress );
//
//                    }
//
//                }
//
//            }
//
//
//        }
//
//        return memoryRefNode;
//    }
//}
