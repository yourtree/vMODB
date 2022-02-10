package dk.ku.di.dms.vms.database.api.modb;

import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionEnum;
import dk.ku.di.dms.vms.database.query.parser.stmt.*;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Bypassing the parsing from strings
 */

// TODO make it private and create an interface (IQueryBuilder) with these methods
public final class QueryBuilder implements IQueryBuilder {

    // a statement for each to avoid casting
    private SelectStatement selectStatement;

    private UpdateStatement updateStatement;

    private AbstractStatement statement;

    // Select

    public QueryBuilder select(String param) throws BuilderException {

        if(statement != null) throw new BuilderException("This builder is already building a statement.");
        this.selectStatement = new SelectStatement();
        this.statement = this.selectStatement;

        String[] projection = param.split(",");
        this.selectStatement.selectClause = new ArrayList<>(Arrays.asList(projection));

        return this;
    }

    public QueryBuilder from(String param) {
        String[] projection = param.split(",");
        this.selectStatement.fromClause = new ArrayList<>(Arrays.asList(projection));
        return this;
    }

    public QueryBuilder join(String param){
        this.statement.join( param );
        return this;
    }

    public QueryBuilder on(String param1, ExpressionEnum expression, String param2) throws BuilderException {
        String[] param2Array = param2.split(".");
        if(param2Array.length != 2) {
            throw new BuilderException("Should contain a table and a column following the pattern <table>.<column>");
        }
        this.statement.on(param1,expression,param2Array[0],param2Array[1]);
        return this;
    }

    // TODO sort and group by

    // Update

    public QueryBuilder update(String param) throws BuilderException {
        if(statement != null) throw new BuilderException("This builder is already building a statement.");
        this.updateStatement = new UpdateStatement();
        this.statement = this.updateStatement;
        this.updateStatement.table = param;
        return this;
    }

    public QueryBuilder set(String param, Object value) {
        SetClauseElement setClauseElement = new SetClauseElement( param, value );
        this.updateStatement.setClause.add(setClauseElement);
        return this;
    }

    // Found in both select and update

    public QueryBuilder where(final String param, final ExpressionEnum expr, final Object value) {
        this.statement.where( param, expr, value );
        return this;
    }

    public QueryBuilder where(String param1, ExpressionEnum expr, String param2) {
        this.statement.where( param1, expr, param2 );
        return this;
    }

    public QueryBuilder and(String param, final ExpressionEnum expr, final Object value) {
        this.statement.and( param, expr, value );
        return this;
    }

    public QueryBuilder or(String param, final ExpressionEnum expr, final Object value) {
        this.statement.or( param, expr, value );
        return this;
    }

    public IStatement build() {

        // TODO check syntax
        // TODO check if tables and columns do exist

        return statement;
    }
}