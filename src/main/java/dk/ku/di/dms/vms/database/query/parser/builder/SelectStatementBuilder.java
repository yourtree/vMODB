package dk.ku.di.dms.vms.database.query.parser.builder;

import dk.ku.di.dms.vms.database.api.modb.IQueryBuilder;
import dk.ku.di.dms.vms.database.query.parser.clause.GroupBySelectElement;
import dk.ku.di.dms.vms.database.query.parser.clause.HavingClauseElement;
import dk.ku.di.dms.vms.database.query.parser.clause.OrderByClauseElement;
import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.database.query.parser.enums.GroupByOperationEnum;
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

    private final EntryPoint entryPoint;

    public SelectStatementBuilder(){
        SelectStatement statement = new SelectStatement();
        this.entryPoint = new EntryPoint(statement);
    }

    /**
     * @param param
     * @return
     */
    public NewProjectionOrFromClause select(String param) {
        return this.entryPoint.select(param);
    }

    public NewProjectionOrFromClause avg(String param){
        return this.entryPoint.avg(param);
    }

    protected class EntryPoint {

        private final SelectStatement statement;
        public EntryPoint(SelectStatement statement){
            this.statement = statement;
        }

        public NewProjectionOrFromClause select(String param) {
            String[] projection = param.replace(" ","").split(",");
            this.statement.selectClause = new ArrayList<>(Arrays.asList(projection));
            return new NewProjectionOrFromClause(this.statement,this);
        }

        public NewProjectionOrFromClause avg(String param){
            GroupBySelectElement element = new GroupBySelectElement( param, GroupByOperationEnum.AVG );
            this.statement.groupBySelectClause = new ArrayList<>();
            this.statement.groupBySelectClause.add( element );
            return new NewProjectionOrFromClause(statement,this);
        }
    }

    public class NewProjectionOrFromClause {

        private final SelectStatement statement;
        private final EntryPoint entryPoint;

        protected NewProjectionOrFromClause(SelectStatement selectStatement, EntryPoint entryPoint){
            this.statement = selectStatement;
            this.entryPoint = entryPoint;
        }

        public NewProjectionOrFromClause select(String param) {
            return this.entryPoint.select(param);
        }

        public NewProjectionOrFromClause avg(String param){
            return this.entryPoint.avg(param);
        }

        public NewProjectionOrFromClause having(ExpressionTypeEnum expression, Number value){
            if(this.statement.havingClause == null) {
                this.statement.havingClause = new ArrayList<>();
            }
            this.statement.havingClause.add( new HavingClauseElement<>(expression,value));
            return this;
        }

        public OrderByGroupByJoinWhereClauseBridge from(String param) {
            String[] projection = param.replace(" ","").split(",");
            this.statement.fromClause = new ArrayList<>(Arrays.asList(projection));
            return new OrderByGroupByJoinWhereClauseBridge(statement);
        }

    }

    public class OrderByGroupByJoinWhereClauseBridge extends JoinWhereClauseBridge<SelectStatement> {

        private final SelectStatement statement;

        protected OrderByGroupByJoinWhereClauseBridge(SelectStatement statement) {
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

            for (String param : params) {
                orderByClauseElements.add(new OrderByClauseElement(param.replace(" ", "")));
            }
            this.statement.orderByClause = orderByClauseElements;

            return new OrderByClausePredicate(this.statement);

        }

    }

    public class OrderByClause implements IQueryBuilder<SelectStatement> {

        private final SelectStatement statement;

        protected OrderByClause(SelectStatement selectStatement){
            this.statement = selectStatement;
        }

        public OrderByClausePredicate orderBy(String... params){

            List<OrderByClauseElement> orderByClauseElements = new ArrayList<>();

            for (String param : params) {
                orderByClauseElements.add(new OrderByClauseElement(param.replace(" ", "")));
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

        protected OrderByClausePredicate(SelectStatement selectStatement){
            this.statement = selectStatement;
        }

//        public OrderByClause asc(){
//
//        }

        public QuerySeal desc(){
            for( OrderByClauseElement predicate : this.statement.orderByClause ){
                predicate.expression = OrderBySortOrderEnum.DESC;
            }
            return new QuerySeal(this.statement);
        }

        public SelectStatement build(){
            return statement;
        }

    }

    // not of my interest to make it static, otherwise developers could instantiate it directly (in case the constructor were public)
    public class QuerySeal implements IQueryBuilder<SelectStatement> {

        private final SelectStatement statement;

        protected QuerySeal(SelectStatement selectStatement){
            this.statement = selectStatement;
        }

        public SelectStatement build(){
            return statement;
        }
    }

}
