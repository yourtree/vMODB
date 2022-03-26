package dk.ku.di.dms.vms.modb.common.query.statement;

import dk.ku.di.dms.vms.modb.common.query.clause.GroupBySelectElement;
import dk.ku.di.dms.vms.modb.common.query.clause.HavingClauseElement;
import dk.ku.di.dms.vms.modb.common.query.clause.OrderByClauseElement;

import java.util.List;

public class SelectStatement extends AbstractStatement {

    public List<String> selectClause;

    public List<GroupBySelectElement> groupBySelectClause;

    public List<HavingClauseElement<?>> havingClause;

    public List<String> fromClause;

    public List<OrderByClauseElement> orderByClause;

    public List<String> groupByClause;

    @Override
    public SelectStatement getAsSelectStatement() {
        return this;
    }

    @Override
    public boolean isSelect(){ return true; }

    // No support for UNION, EXCEPT, INTERSECT yet
}
