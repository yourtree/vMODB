package dk.ku.di.dms.vms.modb.query.analyzer.predicate;

import dk.ku.di.dms.vms.modb.common.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.common.query.enums.JoinTypeEnum;
import dk.ku.di.dms.vms.modb.schema.ColumnReference;
import dk.ku.di.dms.vms.modb.table.Table;

public class JoinPredicate {

    public ColumnReference columnLeftReference;
    public ColumnReference columnRightReference;
    public ExpressionTypeEnum expression;
    public JoinTypeEnum type;

    public JoinPredicate(ColumnReference columnLeftReference,
                         ColumnReference columnRightReference,
                         ExpressionTypeEnum expression,
                         JoinTypeEnum type) {
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
