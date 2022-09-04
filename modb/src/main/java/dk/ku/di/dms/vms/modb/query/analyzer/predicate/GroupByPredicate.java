package dk.ku.di.dms.vms.modb.query.analyzer.predicate;

import dk.ku.di.dms.vms.modb.common.query.enums.GroupByOperationEnum;
import dk.ku.di.dms.vms.modb.definition.ColumnReference;

import java.util.List;

public class GroupByPredicate {

    public final ColumnReference columnReference;
    public final GroupByOperationEnum groupByOperation;

    public GroupByPredicate(ColumnReference columnReference, GroupByOperationEnum groupByOperation) {
        this.columnReference = columnReference;
        this.groupByOperation = groupByOperation;
    }

}
