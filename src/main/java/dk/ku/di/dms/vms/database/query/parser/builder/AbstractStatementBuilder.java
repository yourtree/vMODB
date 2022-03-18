package dk.ku.di.dms.vms.database.query.parser.builder;

import dk.ku.di.dms.vms.database.api.modb.IQueryBuilder;
import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.database.query.parser.enums.JoinTypeEnum;
import dk.ku.di.dms.vms.database.query.parser.stmt.AbstractStatement;
import dk.ku.di.dms.vms.database.query.parser.clause.JoinClauseElement;
import dk.ku.di.dms.vms.database.query.parser.clause.WhereClauseElement;

import java.util.ArrayList;

/**
 * A class that embraces the commonalities found in both SELECT
 * and UPDATE statements such as where and join clauses
 *
 * Bypassing the parsing from strings
 * TODO: look at https://github.com/19WAS85/coollection#readme and
 * Interesting to take a look: https://www.jinq.org/
 */
public abstract class AbstractStatementBuilder {

    /**
     * Since after a FROM clause both WHERE and JOIN clauses can be specified, this class serves as bridge to define what comes next
     */
    public class JoinWhereClauseBridge<T extends AbstractStatement> implements IQueryBuilder<T> {

        protected final T statement;

        protected JoinWhereClauseBridge(T statement){
            this.statement = statement;
        }

        public WhereClausePredicate<T> where(final String param, final ExpressionTypeEnum expr, final Object value) {
            WhereClauseElement<Object> element = new WhereClauseElement<>(param,expr,value);
            this.statement.whereClause.add( element );
            return new WhereClausePredicate<>(this.statement);
        }

        public WhereClausePredicate<T> where(String param1, ExpressionTypeEnum expr, String param2){
            WhereClauseElement<String> element = new WhereClauseElement<>(param1,expr,param2);
            this.statement.whereClause.add( element );
            return new WhereClausePredicate<>(this.statement);
        }

        public JoinClausePredicate<T> join(String table, String column) {
            return new JoinClausePredicate<>(this.statement, table, column);
        }

        public T build(){
            return statement;
        }

    }

    /**
     * A join clause predicate can be proceeded by a where clause, another join or another join condition
     */
    public class CondJoinWhereClauseBridge<T extends AbstractStatement> extends JoinWhereClauseBridge<T> {

        private final JoinClauseElement joinClauseElement;

        protected CondJoinWhereClauseBridge(JoinClauseElement joinClauseElement, T statement){
            super(statement);
            this.joinClauseElement = joinClauseElement;
        }

        public CondJoinWhereClauseBridge<T> and(String columnLeft, ExpressionTypeEnum expression, String columnRight){
            this.joinClauseElement.addCondition( columnLeft, expression, columnRight );
            return new CondJoinWhereClauseBridge<>(joinClauseElement, this.statement);
        }

        // TODO implement later
//        public CondJoinWhereClauseBridge or(){
//
//        }

    }

    public class JoinClausePredicate<T extends AbstractStatement> {

        private final T statement;

        /**
         * The following attributes are used to cache the information received in the previous
         * join method, so it can be used in the "on" method for correctly matching the table and attributes
         */
        private final String table;
        private final String column;
        private final JoinTypeEnum joinType;

        protected JoinClausePredicate(T statement, String table, String column){
            this.statement = statement;
            this.table = table;
            this.column = column;
            this.joinType = JoinTypeEnum.INNER_JOIN;
            this.statement.joinClause = new ArrayList<>();
        }

        public CondJoinWhereClauseBridge<T> on(ExpressionTypeEnum expression, String table, String param) {
            JoinClauseElement joinClauseElement =
                    new JoinClauseElement(this.table,this.column,this.joinType, expression, table, param);
            this.statement.joinClause.add(joinClauseElement);
            // cannot nullify now given I may still need in case of another join condition for this same JOIN
            // this.tempJoinTable = null;
            // this.tempJoinType = null;
            return new CondJoinWhereClauseBridge<>( joinClauseElement, this.statement );
        }

    }

    public class WhereClausePredicate<T extends AbstractStatement> implements IQueryBuilder<T> {

        private final T statement;

        protected WhereClausePredicate(T statement){
            this.statement = statement;
            this.statement.whereClause = new ArrayList<>();
        }

        public WhereClausePredicate<T> and(String param, final ExpressionTypeEnum expr, final Object value) {
            WhereClauseElement<Object> element = new WhereClauseElement<>(param,expr,value);
            this.statement.whereClause.add( element );
            return this;
        }

        public WhereClausePredicate<T> or(String param, final ExpressionTypeEnum expr, final Object value) {
            WhereClauseElement<Object> element = new WhereClauseElement<>(param,expr,value);
            this.statement.whereClause.add( element );
            return this;
        }

        public T build(){
            return statement;
        }

    }

}
