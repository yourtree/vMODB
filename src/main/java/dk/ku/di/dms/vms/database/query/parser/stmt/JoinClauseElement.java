package dk.ku.di.dms.vms.database.query.parser.stmt;

import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.database.query.parser.enums.JoinTypeEnum;

public class JoinClauseElement {

    public final String tableLeft;
    public final String columnLeft;
    public final String tableRight;
    public final String columnRight;
    public final JoinTypeEnum joinType;
    public final ExpressionTypeEnum expression;

    public JoinClauseElement(String tableLeft, String columnLeft, JoinTypeEnum joinType, ExpressionTypeEnum expression, String tableRight, String columnRight) {
        this.tableLeft = tableLeft;
        this.columnLeft = columnLeft;
        this.joinType = joinType;
        this.expression = expression;
        this.tableRight = tableRight;
        this.columnRight = columnRight;
    }
}