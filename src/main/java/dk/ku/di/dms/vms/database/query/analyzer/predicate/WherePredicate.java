package dk.ku.di.dms.vms.database.query.analyzer.predicate;

import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionEnum;
import dk.ku.di.dms.vms.database.store.Column;
import dk.ku.di.dms.vms.database.store.ColumnReference;
import dk.ku.di.dms.vms.database.store.Table;

public class WherePredicate {

    public final ColumnReference columnReference;
    public final ExpressionEnum expression;
    public final Object value;

    public WherePredicate(ColumnReference columnReference, ExpressionEnum expression, Object value) {
        this.columnReference = columnReference;
        this.expression = expression;
        this.value = value;
    }

    public WherePredicate(ColumnReference columnReference, ExpressionEnum expression) {
        this.columnReference = columnReference;
        this.expression = expression;
        // for equals, not equals NULL, value is not necessary
        this.value = null;
    }

    public Table<?,?> getTable() {
        return columnReference.table;
    }

    public Column getColumn(){
        return this.columnReference.column;
    }

}
