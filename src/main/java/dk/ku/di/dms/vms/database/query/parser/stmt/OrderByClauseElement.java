package dk.ku.di.dms.vms.database.query.parser.stmt;

import dk.ku.di.dms.vms.database.query.parser.enums.OrderBySortOrderEnum;

public class OrderByClauseElement {

    public final String column;
    public final OrderBySortOrderEnum expression;

    public OrderByClauseElement(String column, OrderBySortOrderEnum expression) {
        this.column = column;
        this.expression = expression;
    }
}
