package dk.ku.di.dms.vms.modb.api.query.statement;

import dk.ku.di.dms.vms.modb.api.query.clause.GroupBySelectElement;
import dk.ku.di.dms.vms.modb.api.query.clause.HavingClauseElement;
import dk.ku.di.dms.vms.modb.api.query.clause.JoinClauseElement;
import dk.ku.di.dms.vms.modb.api.query.clause.OrderByClauseElement;

import java.util.List;

public final class SelectStatement extends AbstractStatement {

    public List<String> selectClause;

    public List<GroupBySelectElement> groupBySelectClause;

    public List<HavingClauseElement<?>> havingClause;

    public List<String> fromClause;

    public List<JoinClauseElement> joinClause;

    public List<OrderByClauseElement> orderByClause;

    public List<String> groupByClause;

    @Override
    public StatementType getType() {
        return StatementType.SELECT;
    }

    @Override
    public SelectStatement asSelectStatement() {
        return this;
    }

    // No support for UNION, EXCEPT, INTERSECT yet
}
