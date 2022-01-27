package dk.ku.di.dms.vms.database.query.analyzer;

import dk.ku.di.dms.vms.database.query.parser.stmt.SortOperationEnum;
import dk.ku.di.dms.vms.database.store.ColumnReference;

public class SortClause {

    public ColumnReference columnReference;
    public SortOperationEnum sortOperation;

}
