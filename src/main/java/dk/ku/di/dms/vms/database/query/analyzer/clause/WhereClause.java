package dk.ku.di.dms.vms.database.query.analyzer.clause;

import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionEnum;
import dk.ku.di.dms.vms.database.store.ColumnReference;

public class WhereClause<T> {

    public final ColumnReference columnReference;
    public final ExpressionEnum expression;
    public final T value;

    public WhereClause(ColumnReference columnReference, ExpressionEnum expression, T value) {
        this.columnReference = columnReference;
        this.expression = expression;
        this.value = value;
    }
}
