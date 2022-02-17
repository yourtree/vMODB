package dk.ku.di.dms.vms.database.query.analyzer.predicate;

import dk.ku.di.dms.vms.database.query.parser.enums.OrderBySortOrderEnum;
import dk.ku.di.dms.vms.database.store.meta.ColumnReference;

public class OrderByPredicate {

    public ColumnReference columnReference;
    public OrderBySortOrderEnum sortOperation;

}
