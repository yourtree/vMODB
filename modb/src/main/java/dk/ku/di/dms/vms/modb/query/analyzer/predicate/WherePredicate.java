package dk.ku.di.dms.vms.modb.query.analyzer.predicate;

import dk.ku.di.dms.vms.modb.common.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.definition.ColumnReference;
import dk.ku.di.dms.vms.modb.definition.Table;

public class WherePredicate {

    public final ColumnReference columnReference;
    public final ExpressionTypeEnum expression;
    public final Object value;

    public WherePredicate(ColumnReference columnReference, ExpressionTypeEnum expression, Object value) {
        this.columnReference = columnReference;
        this.expression = expression;
        this.value = value;
    }

    public WherePredicate(ColumnReference columnReference, ExpressionTypeEnum expression) {
        this.columnReference = columnReference;
        this.expression = expression;
        // for equals, not equals NULL, value is not necessary
        this.value = null;
    }

    public Table getTable() {
        return columnReference.table;
    }

    public Integer getColumnPosition() {
        return columnReference.columnPosition;
    }

}
