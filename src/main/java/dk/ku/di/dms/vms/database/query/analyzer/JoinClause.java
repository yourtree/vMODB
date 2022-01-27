package dk.ku.di.dms.vms.database.query.analyzer;

import dk.ku.di.dms.vms.database.query.parser.stmt.ExpressionEnum;
import dk.ku.di.dms.vms.database.query.parser.stmt.JoinEnum;
import dk.ku.di.dms.vms.database.store.ColumnReference;

public class JoinClause {

    public ColumnReference columnLeftReference;
    public ColumnReference columnRightReference;
    public ExpressionEnum expression;
    public JoinEnum type;

    public JoinClause(ColumnReference columnLeftReference,
                      ColumnReference columnRightReference,
                      ExpressionEnum expression,
                      JoinEnum type) {
        this.columnLeftReference = columnLeftReference;
        this.columnRightReference = columnRightReference;
        this.expression = expression;
        this.type = type;
    }
}
