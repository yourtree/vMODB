package dk.ku.di.dms.vms.database.query.parser.stmt;

import dk.ku.di.dms.vms.database.query.parser.clause.GroupBySelectElement;
import dk.ku.di.dms.vms.database.query.parser.clause.OrderByClauseElement;

import java.util.List;

public class SelectStatement extends AbstractStatement {

    public List<String> selectClause;

    public List<GroupBySelectElement> groupBySelectClause;

    public List<String> fromClause;

    public List<OrderByClauseElement> orderByClause;

    public List<String> groupByClause;

    // No support for having yet
    // No support for UNION, EXCEPT, INTERSECT yet
}
