package dk.ku.di.dms.vms.database.query.parser.stmt;

import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionTypeEnum;

public class WhereClauseElement<T> {

    public final String column;
    public final ExpressionTypeEnum expression;
    public final T value;

    public WhereClauseElement(String column, ExpressionTypeEnum expression, T value) {
        this.column = column;
        this.expression = expression;
        this.value = value;
    }
}
