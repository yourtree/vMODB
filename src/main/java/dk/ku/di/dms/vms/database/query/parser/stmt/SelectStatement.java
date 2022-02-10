package dk.ku.di.dms.vms.database.query.parser.stmt;

import java.util.List;

/**
 * The clauses in a query
 * https://docs.microsoft.com/en-us/sql/t-sql/queries/queries?view=sql-server-ver15
 */
public class SelectStatement extends AbstractStatement {

    public List<String> selectClause;

    public List<String> fromClause;

    public List<OrderByClauseElement> sortByClause;

    public List<GroupByClauseElement> groupByClause;

    // No support for having yet
    // No support for UNION, EXCEPT, INTERSECT yet

}
