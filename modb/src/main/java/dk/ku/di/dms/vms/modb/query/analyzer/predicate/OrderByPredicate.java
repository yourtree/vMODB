package dk.ku.di.dms.vms.modb.query.analyzer.predicate;

import dk.ku.di.dms.vms.modb.common.query.parser.enums.OrderBySortOrderEnum;
import dk.ku.di.dms.vms.store.meta.ColumnReference;

public class OrderByPredicate {

    public ColumnReference columnReference;
    public OrderBySortOrderEnum sortOperation;

}
