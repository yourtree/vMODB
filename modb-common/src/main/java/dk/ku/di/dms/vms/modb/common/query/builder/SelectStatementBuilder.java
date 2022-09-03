package dk.ku.di.dms.vms.modb.common.query.builder;

import dk.ku.di.dms.vms.modb.common.query.clause.*;
import dk.ku.di.dms.vms.modb.common.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.common.query.enums.GroupByOperationEnum;
import dk.ku.di.dms.vms.modb.common.query.enums.JoinTypeEnum;
import dk.ku.di.dms.vms.modb.common.query.enums.OrderBySortOrderEnum;
import dk.ku.di.dms.vms.modb.common.query.statement.SelectStatement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The clauses from a SELECT statement. Additional clauses found in {@link AbstractStatementBuilder}
 * https://docs.microsoft.com/en-us/sql/t-sql/queries/queries?view=sql-server-ver15
 */
public class SelectStatementBuilder extends AbstractStatementBuilder  {

    private final EntryPoint entryPoint;

    protected SelectStatementBuilder(){
        SelectStatement statement = new SelectStatement();
        this.entryPoint = new EntryPoint(statement);
    }

    /**
     * @param param The column name
     * @return Resulting select with column
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
            this.statement.SQL.append(param);
            return new NewProjectionOrFromClause(this.statement,this);
        }

        public NewProjectionOrFromClause avg(String param){
            GroupBySelectElement element = new GroupBySelectElement( param, GroupByOperationEnum.AVG );
            this.statement.groupBySelectClause = new ArrayList<>();
            this.statement.groupBySelectClause.add( element );
            this.statement.SQL.append(param);
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
            this.statement.SQL.append(param);
            return new OrderByGroupByJoinWhereClauseBridge(statement);
        }

    }

    public class JoinClausePredicate {

        private final SelectStatement statement;

        /**
         * The following attributes are used to cache the information received in the previous
         * join method, so it can be used in the "on" method for correctly matching the table and attributes
         */
        private final String table;
        private final String column;
        private final JoinTypeEnum joinType;

        protected JoinClausePredicate(SelectStatement statement, String table, String column){
            this.statement = statement;
            this.table = table;
            this.column = column;
            this.joinType = JoinTypeEnum.INNER_JOIN;
            this.statement.joinClause = new ArrayList<>();
        }

        public CondJoinWhereClauseBridge on(ExpressionTypeEnum expression, String table, String columnParam) {
            JoinClauseElement joinClauseElement =
                    new JoinClauseElement(this.table,this.column,this.joinType, expression, table, columnParam);
            this.statement.joinClause.add(joinClauseElement);
            /*
               cannot nullify now given I may still need in case of another join condition for this same JOIN
               this.tempJoinTable = null;
               this.tempJoinType = null;
            */
            this.statement.SQL.append(this.table);
            this.statement.SQL.append(this.column);
            this.statement.SQL.append(this.joinType.name);
            this.statement.SQL.append(table);
            this.statement.SQL.append(columnParam);
            return new CondJoinWhereClauseBridge( joinClauseElement, this.statement );
        }

    }

    /**
     * Since after a FROM clause both WHERE and JOIN clauses can be specified, this class serves as bridge to define what comes next
     */
    public class JoinWhereClauseBridge implements IQueryBuilder<SelectStatement> {

        protected final SelectStatement statement;

        protected JoinWhereClauseBridge(SelectStatement statement){
            this.statement = statement;
        }

        public WhereClausePredicate<SelectStatement> where(final String param, final ExpressionTypeEnum expr, final Object value) {
            WhereClauseElement<Object> element = new WhereClauseElement<>(param,expr,value);
            this.statement.whereClause.add( element );
            this.statement.SQL.append(param);
            this.statement.SQL.append(expr.name);
            this.statement.SQL.append('?');
            return new WhereClausePredicate<>(this.statement);
        }

        public WhereClausePredicate<SelectStatement> where(String param1, ExpressionTypeEnum expr, String param2){
            WhereClauseElement<String> element = new WhereClauseElement<>(param1,expr,param2);
            this.statement.whereClause.add( element );
            this.statement.SQL.append(param1);
            this.statement.SQL.append(expr.name);
            this.statement.SQL.append(param2);
            return new WhereClausePredicate<>(this.statement);
        }

        public JoinClausePredicate join(String table, String column) {
            return new JoinClausePredicate(this.statement, table, column);
        }

        public SelectStatement build(){
            return statement;
        }

    }

    /**
     * A join clause predicate can be proceeded by a where clause, another join or another join condition
     */
    public class CondJoinWhereClauseBridge extends JoinWhereClauseBridge {

        private final JoinClauseElement joinClauseElement;

        protected CondJoinWhereClauseBridge(JoinClauseElement joinClauseElement, SelectStatement statement){
            super(statement);
            this.joinClauseElement = joinClauseElement;
        }

        public CondJoinWhereClauseBridge and(String columnLeft, ExpressionTypeEnum expression, String columnRight){
            this.joinClauseElement.addCondition( columnLeft, expression, columnRight );
            return new CondJoinWhereClauseBridge(joinClauseElement, this.statement);
        }

        // TODO JOIN with OR condition implement later or leave like this?
//        public CondJoinWhereClauseBridge or(){
//
//        }

    }

    public class OrderByGroupByJoinWhereClauseBridge extends JoinWhereClauseBridge {

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
            this.statement.SQL.append(params);
            return new OrderByClause(this.statement);
        }

        public OrderByClausePredicate orderBy(String... params){

            List<OrderByClauseElement> orderByClauseElements = new ArrayList<>();

            for (String param : params) {
                orderByClauseElements.add(new OrderByClauseElement(param.replace(" ", "")));
            }
            this.statement.orderByClause = orderByClauseElements;
            this.statement.SQL.append(params);
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
            this.statement.SQL.append(params);
            return new OrderByClausePredicate(this.statement);

        }

        public SelectStatement build(){
            return statement;
        }

    }

    public static class OrderByClausePredicate implements IQueryBuilder<SelectStatement> {

        private final SelectStatement statement;

        protected OrderByClausePredicate(SelectStatement selectStatement){
            this.statement = selectStatement;
        }

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

    public static class QuerySeal implements IQueryBuilder<SelectStatement> {

        private final SelectStatement statement;

        protected QuerySeal(SelectStatement selectStatement){
            this.statement = selectStatement;
        }

        public SelectStatement build(){
            return statement;
        }
    }

}
