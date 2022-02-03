package dk.ku.di.dms.vms.database.query.analyzer.clause;

import dk.ku.di.dms.vms.database.query.parser.enums.SortEnum;
import dk.ku.di.dms.vms.database.store.ColumnReference;

public class SortPredicate {

    public ColumnReference columnReference;
    public SortEnum sortOperation;

}
