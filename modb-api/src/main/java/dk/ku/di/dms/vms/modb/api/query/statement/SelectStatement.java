package dk.ku.di.dms.vms.modb.api.query.statement;

import dk.ku.di.dms.vms.modb.api.query.clause.*;

import java.util.List;

/**
 * No support for UNION, EXCEPT, INTERSECT yet
 */
public final class SelectStatement extends AbstractStatement {

    public List<String> selectClause;

    public List<GroupBySelectElement> groupBySelectClause;

    public List<HavingClauseElement<?>> havingClause;

    public List<String> fromClause;

    public List<JoinClauseElement> joinClause;

    public List<OrderByClauseElement> orderByClause;

    public List<String> groupByClause;

    public SelectStatement() {}

    public SelectStatement(List<String> selectClause, String table, List<WhereClauseElement<?>> whereClause) {
        this.selectClause = selectClause;
        this.fromClause = List.of(table);
        this.whereClause = whereClause;
    }

    @Override
    public StatementType getType() {
        return StatementType.SELECT;
    }

    @Override
    public SelectStatement asSelectStatement() {
        return this;
    }

}
