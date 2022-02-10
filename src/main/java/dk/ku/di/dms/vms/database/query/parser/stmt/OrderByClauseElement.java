package dk.ku.di.dms.vms.database.query.parser.stmt;

import dk.ku.di.dms.vms.database.query.parser.enums.OrderBySortEnum;

public class OrderByClauseElement {

    public final String column;
    public final OrderBySortEnum expression;

    public OrderByClauseElement(String column, OrderBySortEnum expression) {
        this.column = column;
        this.expression = expression;
    }
}
