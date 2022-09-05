package dk.ku.di.dms.vms.modb.query.analyzer.predicate;

import dk.ku.di.dms.vms.modb.api.query.enums.GroupByOperationEnum;
import dk.ku.di.dms.vms.modb.definition.ColumnReference;

public class GroupByPredicate {

    public final ColumnReference columnReference;
    public final GroupByOperationEnum groupByOperation;

    public GroupByPredicate(ColumnReference columnReference, GroupByOperationEnum groupByOperation) {
        this.columnReference = columnReference;
        this.groupByOperation = groupByOperation;
    }

}
