package dk.ku.di.dms.vms.database.store.refac;

import java.util.List;
import java.util.Map;

public abstract class Table {

    public String name;

    public Map<String, Integer> columnToPointerMap;

    public Column[] columns;

    public Map<Integer,Row> rows;

    public abstract int size();

    // the principle is that I will always have the primary key indexed

//    public Row(List<Integer> positions){
//        StringBuilder sb = new StringBuilder();
//        positions.stream().forEach( pos -> {
//            sb.append(data[pos]);
//        });
//        this.primaryKey = sb.toString().hashCode();
//    }
//
//    public Row(final int pos){
//        // assert data != null;
//        this.primaryKey = data[pos] instanceof Integer ? (int) data[pos] : data[pos].hashCode();
//    }


}
