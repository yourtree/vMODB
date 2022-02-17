package dk.ku.di.dms.vms.database.query.analyzer.predicate;

import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionEnum;
import dk.ku.di.dms.vms.database.query.parser.enums.JoinEnum;
import dk.ku.di.dms.vms.database.store.meta.ColumnReference;
import dk.ku.di.dms.vms.database.store.table.Table;

public class JoinPredicate {

    public ColumnReference columnLeftReference;
    public ColumnReference columnRightReference;
    public ExpressionEnum expression;
    public JoinEnum type;

    public JoinPredicate(ColumnReference columnLeftReference,
                         ColumnReference columnRightReference,
                         ExpressionEnum expression,
                         JoinEnum type) {
        this.columnLeftReference = columnLeftReference;
        this.columnRightReference = columnRightReference;
        this.expression = expression;
        this.type = type;
    }

    public Table getLeftTable() {
        return columnLeftReference.table;
    }

    public Table getRightTable() {
        return columnRightReference.table;
    }

}
