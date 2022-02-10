package dk.ku.di.dms.vms.database.query.parser.stmt;

import dk.ku.di.dms.vms.database.query.parser.enums.GroupByAggregateFunctionEnum;

public class GroupByClauseElement {

    public final String column;
    public final GroupByAggregateFunctionEnum expression;

    public GroupByClauseElement(final String column) {
        this.column = column;
        this.expression = null;
    }

    /** If expression exists */
    public GroupByClauseElement(final String column, final GroupByAggregateFunctionEnum expression) {
        this.column = column;
        this.expression = expression;
    }

}
