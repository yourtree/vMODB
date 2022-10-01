//package dk.ku.di.dms.vms.modb.index;
//
//import dk.ku.di.dms.vms.modb.common.type.DataType;
//import dk.ku.di.dms.vms.modb.definition.Row;
//import dk.ku.di.dms.vms.modb.definition.Table;
//
//import java.util.Collection;
//import java.util.SortedMap;
//import java.util.TreeMap;
//
///**
// * Basic implementation of a range index
// * Given a column and a data type
// */
//public class RangeIndex<T extends Comparable> { //extends AbstractIndex<T> {
//
//    // TODO a range index can also be created using a condition
//
//    private final SortedMap<T,Collection<Row>> map;
//
//    private final int columnIndex;
//
//    private long counter;
//
//    public RangeIndex(final DataType dataType, final Table table, final int columnIndex){
//        // super(table, columnIndex);
//        this.map = this.getMap(dataType);
//        this.columnIndex = columnIndex;
//    }
//
//    private SortedMap<T,Collection<Row>> getMap(final DataType dataType){
//
//        switch(dataType){
//            case INT:
//                return (SortedMap<T, Collection<Row>>) new TreeMap<Integer,Collection<Row>>();
//            case CHAR:
//                return (SortedMap<T, Collection<Row>>) new TreeMap<Character,Collection<Row>>();
//            case LONG:
//                return (SortedMap<T, Collection<Row>>) new TreeMap<Long,Collection<Row>>();
//            case DOUBLE:
//                return (SortedMap<T, Collection<Row>>) new TreeMap<Double,Collection<Row>>();
//            default:
//                throw new IllegalStateException("Unexpected value: " + dataType);
//        }
//    }
//
//
////    @Override
////    public boolean upsertImpl(T key, Row row) {
////        // TODO finish
////        return true;
////    }
////
////    @Override
////    public boolean delete(T key) {
////        map.remove(key);
////        return true;
////    }
////
////    @Override
////    public Row retrieve(T key) {
////        // returns the first row
////        if(map.get(key) != null && map.get(key).size() > 0){
////            return map.get(key).iterator().next();
////        }
////        return null;
////    }
////
////    @Override
////    public Collection<Row> retrieveCollection(T key) {
////        return map.get(key);
////    }
////
////    @Override
////    public boolean retrieve(T key, Row outputRow) {
////        outputRow = this.retrieve(key);
////        return outputRow != null;
////    }
////
////    @Override
////    public int size() {
////        // TODO actually should not be map.size(), but a counter that I should maintain
////        return map.size();
////    }
////
////    @Override
////    public Collection<Row> rows() {
////        // for a range index, this is an expensive operation
////        // all collections require being aggregated
////        // this index should be avoided in case no index can be applied
////        return map.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
////    }
////
////    @Override
////    public IndexDataStructureEnum getType() {
////        return IndexDataStructureEnum.TREE;
////    }
////
////    @Override
////    public Set<Map.Entry<T, Row>> entrySet() {
////        return null;
////    }
//
//}
