package dk.ku.di.dms.vms.database.query.parser.clause;

public class SetClauseElement {

    public final String column;
    public final Object value;

    public SetClauseElement(String column, Object value) {
        this.column = column;
        this.value = value;
    }
}
