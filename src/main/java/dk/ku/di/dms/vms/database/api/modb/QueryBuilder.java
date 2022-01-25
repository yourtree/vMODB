package dk.ku.di.dms.vms.database.api.modb;

import dk.ku.di.dms.vms.database.query.parse.*;

import java.util.ArrayList;
import java.util.Arrays;

// TODO make it private and create an interface (IQueryBuilder) with these methods
public final class QueryBuilder {

    // to avoid casting
    private SelectStatement selectStmt;

    private UpdateStatement updateStmt;

    // TODO throw exception if two consecutive invalid declarations are used
    // e.g., FROM and FROM, SELECT and SELECT
    // private Op lastOp;

//        private QueryBuilder() {
//            this.query = new StringBuilder();
//        }

    public QueryBuilder select(String param) {

        this.selectStmt = new SelectStatement();
        String[] projection = param.split(",");
        this.selectStmt.projection = new ArrayList<>(Arrays.asList(projection));

        return this;
    }

    public QueryBuilder from(String param) {

        String[] projection = param.split(",");
        this.selectStmt.fromClause = new ArrayList<>(Arrays.asList(projection));

        return this;
    }

    public QueryBuilder where(final String param, final ExpressionEnum expr, final Object value) {
        this.selectStmt.whereClause = new ArrayList<>();
        WhereClauseElement element = new WhereClauseElement(param,expr,value);
        this.selectStmt.whereClause.add( element );
        return this;
    }

    public QueryBuilder and(String param, final ExpressionEnum expr, final Object value) {
        WhereClauseElement element = new WhereClauseElement(param,expr,value);
        this.selectStmt.whereClause.add( element );
        return this;
    }

    public QueryBuilder or(String param, final ExpressionEnum expr, final Object value) {
        WhereClauseElement element = new WhereClauseElement(param,expr,value);
        this.selectStmt.whereClause.add( element );
        return this;
    }

    public QueryBuilder join(String param, Object value) {
        // query.append(" JOIN ").append(param).append(" ON ").append(value);
        return this;
    }

    public QueryBuilder update(String param) {
        // query.append("UPDATE ").append(param);

        this.updateStmt = new UpdateStatement();
        this.updateStmt.table = param;

        return this;
    }

    public QueryBuilder set(String param, Object value) {
        // query.append(" SET ").append(param).append(value);

        SetClauseElement setClauseElement = new SetClauseElement( param, value );

        this.updateStmt.setClause.add(setClauseElement);

        return this;
    }

    public Statement build() {

        // TODO check syntax
        // TODO check if tables and columns do exist



        return selectStmt != null ? selectStmt : updateStmt;
    }
}