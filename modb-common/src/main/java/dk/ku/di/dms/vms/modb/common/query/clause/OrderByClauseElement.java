package dk.ku.di.dms.vms.modb.common.query.clause;

import dk.ku.di.dms.vms.modb.common.query.enums.OrderBySortOrderEnum;

public class OrderByClauseElement {

    public final String column;
    public OrderBySortOrderEnum expression;

    public OrderByClauseElement(String column, OrderBySortOrderEnum expression) {
        this.column = column;
        this.expression = expression;
    }

    public OrderByClauseElement(String column) {
        this.column = column;
        this.expression = OrderBySortOrderEnum.ASC;
    }

}
