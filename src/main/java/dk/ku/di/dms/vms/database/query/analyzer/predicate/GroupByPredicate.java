package dk.ku.di.dms.vms.database.query.analyzer.predicate;

import dk.ku.di.dms.vms.database.query.parser.enums.GroupByOperationEnum;
import dk.ku.di.dms.vms.database.store.meta.ColumnReference;

public class GroupByPredicate {

    public final ColumnReference columnReference;
    public final GroupByOperationEnum groupOperation;

    public GroupByPredicate(ColumnReference columnReference, GroupByOperationEnum groupOperation) {
        this.columnReference = columnReference;
        this.groupOperation = groupOperation;
    }
}
