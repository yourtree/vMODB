package dk.ku.di.dms.vms.database.query.parser.stmt;

import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.database.query.parser.enums.JoinTypeEnum;
import dk.ku.di.dms.vms.database.query.parser.enums.StatementTypeEnum;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractStatement implements IStatement {

    public List<WhereClauseElement<?>> whereClause;

    public List<JoinClauseElement> joinClause;
    private String tempJoinTable;
    private JoinTypeEnum tempJoinType;

    private StatementTypeEnum lastStatement = null;

    public void where(final String param, final ExpressionTypeEnum expr, final Object value) {
        if (this.whereClause == null) this.whereClause = new ArrayList<>();
        WhereClauseElement<Object> element = new WhereClauseElement<>(param,expr,value);
        this.whereClause.add( element );
    }

    public void where(String param1, ExpressionTypeEnum expr, String param2){
        if (this.whereClause == null) this.whereClause = new ArrayList<>();
        WhereClauseElement<String> element = new WhereClauseElement<>(param1,expr,param2);
        this.whereClause.add( element );
    }

    public void and(String param, final ExpressionTypeEnum expr, final Object value) {
        WhereClauseElement<Object> element = new WhereClauseElement<>(param,expr,value);
        this.whereClause.add( element );
    }

    /** TODO finish
     * Only used for JOIN clause. E.g., join .. on .. and ..
     * @param param1
     * @param expr
     * @param param2
     */
    public void and(String param1, final ExpressionTypeEnum expr, final String param2) {
        // if(lastStatement != ON) throw new Exception("")
        // WhereClauseElement<Object> element = new WhereClauseElement<>(param,expr,value);
        // this.whereClause.add( element );
    }

    public void or(String param, final ExpressionTypeEnum expr, final Object value) {
        WhereClauseElement<Object> element = new WhereClauseElement<>(param,expr,value);
        this.whereClause.add( element );
    }

    // join

    public void join(String param) {
        this.tempJoinTable = param;
        this.tempJoinType = JoinTypeEnum.JOIN;
    }

    public void on(String param1, ExpressionTypeEnum expression, String table2, String param2) {
        JoinClauseElement joinClauseElement =
                new JoinClauseElement(tempJoinTable,param1,tempJoinType, expression, table2, param2);
        if (this.joinClause == null) {
            this.joinClause = new ArrayList<>();
        }
        this.joinClause.add(joinClauseElement);

        // cannot nullify now given I may still need in case of another join condition for this same JOIN
        // this.tempJoinTable = null;
        // this.tempJoinType = null;
    }

}
