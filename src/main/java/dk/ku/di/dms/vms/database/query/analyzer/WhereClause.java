package dk.ku.di.dms.vms.database.query.analyzer;

import dk.ku.di.dms.vms.database.query.parser.stmt.ExpressionEnum;
import dk.ku.di.dms.vms.database.store.ColumnReference;

public class WhereClause {

    public ColumnReference columnReference;
    public ExpressionEnum expression;
    public Object value;

}
