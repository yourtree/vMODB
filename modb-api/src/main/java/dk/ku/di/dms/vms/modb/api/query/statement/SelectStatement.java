package dk.ku.di.dms.vms.modb.api.query.statement;

import dk.ku.di.dms.vms.modb.api.query.clause.*;
import dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum;

import java.util.List;

/**
 * No support for UNION, EXCEPT, INTERSECT yet.
 * This class is immutable. That is, by the time it is created, none of its structure can be changed.
 * However, to avoid the overhead on creating the same statement on every call,
 * this class can be created as static on a class annotated as {@link dk.ku.di.dms.vms.modb.api.annotations.Microservice},
 * and benefit of parameterized calls
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
