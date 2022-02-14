package dk.ku.di.dms.vms.database.query.parser.stmt;

import dk.ku.di.dms.vms.database.query.parser.enums.GroupByEnum;

public class GroupByClauseElement {

    public final String column;
    public final GroupByEnum expression;

    public GroupByClauseElement(final String column) {
        this.column = column;
        this.expression = null;
    }

    /** If expression exists */
    public GroupByClauseElement(final String column, final GroupByEnum expression) {
        this.column = column;
        this.expression = expression;
    }

}
