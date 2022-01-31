package dk.ku.di.dms.vms.database.query.analyzer.clause;

import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionEnum;
import dk.ku.di.dms.vms.database.store.Column;
import dk.ku.di.dms.vms.database.store.ColumnReference;
import dk.ku.di.dms.vms.database.store.Table;

public class WhereClause {

    public final ColumnReference columnReference;
    public final ExpressionEnum expression;
    public final Object value;

    public WhereClause(ColumnReference columnReference, ExpressionEnum expression, Object value) {
        this.columnReference = columnReference;
        this.expression = expression;
        this.value = value;
    }

    public Table<?,?> getTable() {
        return columnReference.table;
    }

    public Column getColumn(){
        return this.columnReference.column;
    }

}
