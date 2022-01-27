package dk.ku.di.dms.vms.database.api.modb;

import dk.ku.di.dms.vms.database.api.IQueryBuilder;
import dk.ku.di.dms.vms.database.query.parser.stmt.*;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Bypassing the parsing from strings
 */

// TODO make it private and create an interface (IQueryBuilder) with these methods
public final class QueryBuilder implements IQueryBuilder {

    // a statement for each to avoid casting
    private SelectStatement selectStmt;

    private UpdateStatement updateStmt;

    // private IStatement statement;

    // Select

    public QueryBuilder select(String param) throws BuilderException {

        if(updateStmt != null) throw new BuilderException("This builder is building an update statement.");

        this.selectStmt = new SelectStatement();
        String[] projection = param.split(",");
        this.selectStmt.columns = new ArrayList<>(Arrays.asList(projection));

        return this;
    }

    public QueryBuilder from(String param) {

        String[] projection = param.split(",");
        this.selectStmt.fromClause = new ArrayList<>(Arrays.asList(projection));

        return this;
    }

    private String tempJoinTable;
    private JoinEnum tempJoinType;

    public QueryBuilder join(String param) {
        this.tempJoinTable = param;
        this.tempJoinType = JoinEnum.JOIN;
        return this;
    }

    public QueryBuilder on(String param1, ExpressionEnum expression, String param2) throws BuilderException {
        String[] param2Array = param2.split(".");

        // cannot be the same table. two tables can have the same column name on join e.g., o_id
        // tableLeft == tableRight throw exception
//        if(tempJoinTable.equalsIgnoreCase( param2Array[0] )) {
//            throw new BuilderException("Cannot join the same tables?"); // actually we can do it
//        }

        if(param2Array.length != 2)
            throw new BuilderException("Should contain a table and a column following the pattern <table>.<column>");

        JoinClauseElement joinClauseElement = new JoinClauseElement(tempJoinTable,param1,tempJoinType, expression, param2Array[0],param2Array[1]);
        if (this.selectStmt.joinClause == null) this.selectStmt.joinClause = new ArrayList<>();
        this.selectStmt.joinClause.add(joinClauseElement);
        this.tempJoinTable = null;
        this.tempJoinType = null;
        return this;
    }

    // Update

    public QueryBuilder update(String param) throws BuilderException {

        if(selectStmt != null) throw new BuilderException("This builder is building a select statement.");

        this.updateStmt = new UpdateStatement();
        this.updateStmt.table = param;

        return this;
    }

    public QueryBuilder set(String param, Object value) {

        SetClauseElement setClauseElement = new SetClauseElement( param, value );
        this.updateStmt.setClause.add(setClauseElement);

        return this;
    }

    // Found in both select and update

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

    public IStatement build() {

        // TODO check syntax
        // TODO check if tables and columns do exist

        return selectStmt != null ? selectStmt : updateStmt;
    }
}