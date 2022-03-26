package dk.ku.di.dms.vms.modb.query.analyzer.predicate;

import dk.ku.di.dms.vms.modb.common.query.enums.GroupByOperationEnum;
import dk.ku.di.dms.vms.modb.store.meta.ColumnReference;

import java.util.List;

public class GroupByPredicate {

    public final ColumnReference columnReference;
    public final GroupByOperationEnum groupOperation;
    public final List<ColumnReference> groupByColumnsReference;

    public GroupByPredicate(ColumnReference columnReference, GroupByOperationEnum groupOperation) {
        this.columnReference = columnReference;
        this.groupOperation = groupOperation;
        this.groupByColumnsReference = null;
    }

    public GroupByPredicate(ColumnReference columnReference, GroupByOperationEnum groupOperation, List<ColumnReference> groupByColumnsReference) {
        this.columnReference = columnReference;
        this.groupOperation = groupOperation;
        this.groupByColumnsReference = groupByColumnsReference;
    }

}
