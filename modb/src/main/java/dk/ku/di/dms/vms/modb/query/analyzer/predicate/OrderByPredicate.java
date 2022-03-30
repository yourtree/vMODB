package dk.ku.di.dms.vms.modb.query.analyzer.predicate;

import dk.ku.di.dms.vms.modb.common.query.enums.OrderBySortOrderEnum;
import dk.ku.di.dms.vms.modb.store.meta.ColumnReference;

public class OrderByPredicate {

    public ColumnReference columnReference;
    public OrderBySortOrderEnum sortOperation;

}
