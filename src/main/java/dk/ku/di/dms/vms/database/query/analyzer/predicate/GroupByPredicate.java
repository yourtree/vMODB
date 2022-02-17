package dk.ku.di.dms.vms.database.query.analyzer.predicate;

import dk.ku.di.dms.vms.database.query.parser.enums.GroupByOperationEnum;
import dk.ku.di.dms.vms.database.store.meta.ColumnReference;

public class GroupByPredicate {

    public ColumnReference columnReference;
    public GroupByOperationEnum groupOperation;

}
