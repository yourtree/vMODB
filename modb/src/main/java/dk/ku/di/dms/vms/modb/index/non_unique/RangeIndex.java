package dk.ku.di.dms.vms.modb.index.non_unique;

import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.index.non_unique.b_plus_tree.NonLeafNode;

/**
 * Basic implementation of a range index
 * Given a column and a data type
 */
public class RangeIndex<T extends Comparable> {

    private static final int DEFAULT_PAGE_SIZE = 1024;

    private final int[] columnIndex;

    private long counter;

    private NonLeafNode parent;

    public RangeIndex( Table table, int pageSize, int... columnIndex){
        // super(table, columnIndex);

        this.columnIndex = columnIndex;
    }

}
