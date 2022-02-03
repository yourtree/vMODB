package dk.ku.di.dms.vms.database.query.analyzer.clause;

import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionEnum;
import dk.ku.di.dms.vms.database.query.parser.enums.JoinEnum;
import dk.ku.di.dms.vms.database.store.ColumnReference;

public class JoinOperation {

    public ColumnReference columnLeftReference;
    public ColumnReference columnRightReference;
    public ExpressionEnum expression;
    public JoinEnum type;

    public JoinOperation(ColumnReference columnLeftReference,
                         ColumnReference columnRightReference,
                         ExpressionEnum expression,
                         JoinEnum type) {
        this.columnLeftReference = columnLeftReference;
        this.columnRightReference = columnRightReference;
        this.expression = expression;
        this.type = type;
    }
}
