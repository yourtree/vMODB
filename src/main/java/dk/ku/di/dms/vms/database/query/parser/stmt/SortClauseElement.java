package dk.ku.di.dms.vms.database.query.parser.stmt;

public class SortClauseElement {

    public final String column;
    public final SortOperationEnum expression;

    public SortClauseElement(String column, SortOperationEnum expression) {
        this.column = column;
        this.expression = expression;
    }
}
