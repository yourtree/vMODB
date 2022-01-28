package dk.ku.di.dms.vms.database.query.parser.stmt;

import dk.ku.di.dms.vms.database.query.parser.enums.SortEnum;

public class SortClauseElement {

    public final String column;
    public final SortEnum expression;

    public SortClauseElement(String column, SortEnum expression) {
        this.column = column;
        this.expression = expression;
    }
}
