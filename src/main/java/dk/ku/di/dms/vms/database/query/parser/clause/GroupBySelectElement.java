package dk.ku.di.dms.vms.database.query.parser.clause;

import dk.ku.di.dms.vms.database.query.parser.enums.GroupByOperationEnum;

public class GroupBySelectElement {

    public final String column;
    public final GroupByOperationEnum operation;

    public GroupBySelectElement(String column, GroupByOperationEnum operation) {
        this.column = column;
        this.operation = operation;
    }

}
