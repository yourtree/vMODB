package dk.ku.di.dms.vms.database.query.parser.stmt;

public class JoinClauseElement {

    public final String tableLeft;
    public final String columnLeft;
    public final String tableRight;
    public final String columnRight;
    public final JoinEnum joinType;
    public final ExpressionEnum expression;

    public JoinClauseElement(String tableLeft, String columnLeft, JoinEnum joinType, ExpressionEnum expression, String tableRight, String columnRight) {
        this.tableLeft = tableLeft;
        this.columnLeft = columnLeft;
        this.joinType = joinType;
        this.expression = expression;
        this.tableRight = tableRight;
        this.columnRight = columnRight;
    }
}