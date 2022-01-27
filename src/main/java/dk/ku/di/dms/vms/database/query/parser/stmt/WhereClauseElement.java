package dk.ku.di.dms.vms.database.query.parser.stmt;

public class WhereClauseElement {

    public final String column;
    public final ExpressionEnum expression;
    public final Object value;

    public WhereClauseElement(String column, ExpressionEnum expression, Object value) {
        this.column = column;
        this.expression = expression;
        this.value = value;
    }
}
