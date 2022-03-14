package dk.ku.di.dms.vms.database.query.parser.builder;

import dk.ku.di.dms.vms.database.api.modb.IQueryBuilder;
import dk.ku.di.dms.vms.database.query.parser.clause.OrderByClauseElement;
import dk.ku.di.dms.vms.database.query.parser.enums.OrderBySortOrderEnum;
import dk.ku.di.dms.vms.database.query.parser.stmt.SelectStatement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The clauses from a SELECT statement. Additional clauses found in {@link AbstractStatementBuilder}
 * https://docs.microsoft.com/en-us/sql/t-sql/queries/queries?view=sql-server-ver15
 */
public class SelectStatementBuilder extends AbstractStatementBuilder  {

    private final SelectStatement statement;

    public SelectStatementBuilder(){
        this.statement = new SelectStatement();
    }

    /**
     * TODO later, this method should return a type that allows both aggregates and from clause being specified
     * @param param
     * @return
     */
    public FromClause select(String param) {
        String[] projection = param.replace(" ","").split(",");
        this.statement.selectClause = new ArrayList<>(Arrays.asList(projection));
        return new FromClause(this.statement);
    }

    public class FromClause {

        private final SelectStatement statement;

        public FromClause(SelectStatement selectStatement){
            this.statement = selectStatement;
        }

        public OrderByGroupByJoinWhereClauseBridge from(String param) {
            String[] projection = param.replace(" ","").split(",");
            this.statement.fromClause = new ArrayList<>(Arrays.asList(projection));
            return new OrderByGroupByJoinWhereClauseBridge(statement);
        }

    }

    public class OrderByGroupByJoinWhereClauseBridge extends JoinWhereClauseBridge<SelectStatement> {

        private final SelectStatement statement;

        public OrderByGroupByJoinWhereClauseBridge(SelectStatement statement) {
            super(statement);
            this.statement = statement; // to avoid cast
        }

        public OrderByClause groupBy(String... params) {

            for( int i = 0; i < params.length; i++ ) {
                params[i] = params[i].replace(" ", "");
            }
            this.statement.groupByClause = new ArrayList<>(Arrays.asList(params));

            return new OrderByClause(this.statement);
        }

        public OrderByClausePredicate orderBy(String... params){

            List<OrderByClauseElement> orderByClauseElements = new ArrayList<>();

            for( int i = 0; i < params.length; i++ ) {
                orderByClauseElements.add( new OrderByClauseElement( params[i].replace(" ", "") ) );
            }
            this.statement.orderByClause = orderByClauseElements;

            return new OrderByClausePredicate(this.statement);

        }

    }

    public class OrderByClause implements IQueryBuilder<SelectStatement> {

        private final SelectStatement statement;

        public OrderByClause(SelectStatement selectStatement){
            this.statement = selectStatement;
        }

        public OrderByClausePredicate orderBy(String... params){

            List<OrderByClauseElement> orderByClauseElements = new ArrayList<>();

            for( int i = 0; i < params.length; i++ ) {
                orderByClauseElements.add( new OrderByClauseElement( params[i].replace(" ", "") ) );
            }
            this.statement.orderByClause = orderByClauseElements;

            return new OrderByClausePredicate(this.statement);

        }

        public SelectStatement build(){
            return statement;
        }

    }

    public class OrderByClausePredicate implements IQueryBuilder<SelectStatement> {

        private final SelectStatement statement;

        public OrderByClausePredicate(SelectStatement selectStatement){
            this.statement = selectStatement;
        }

        // default
//        public OrderByClause asc(){
//
//        }

        public TheEnd desc(){
            for( OrderByClauseElement pred : this.statement.orderByClause ){
                pred.expression = OrderBySortOrderEnum.DESC;
            }
            return new TheEnd(this.statement);
        }

        public SelectStatement build(){
            return statement;
        }

    }

    public class TheEnd implements IQueryBuilder<SelectStatement> {

        private final SelectStatement statement;

        public TheEnd(SelectStatement selectStatement){
            this.statement = selectStatement;
        }

        public SelectStatement build(){
            return statement;
        }
    }

}
