package dk.ku.di.dms.vms.database.query.parser.stmt;


import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionEnum;
import dk.ku.di.dms.vms.database.query.parser.enums.JoinEnum;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractStatement implements IStatement {

    public List<WhereClauseElement> whereClause;

    public List<JoinClauseElement> joinClause;
    private String tempJoinTable;
    private JoinEnum tempJoinType;

    public void where(final String param, final ExpressionEnum expr, final Object value) {
        this.whereClause = new ArrayList<>();
        WhereClauseElement element = new WhereClauseElement(param,expr,value);
        this.whereClause.add( element );
    }

    public void and(String param, final ExpressionEnum expr, final Object value) {
        WhereClauseElement element = new WhereClauseElement(param,expr,value);
        this.whereClause.add( element );
    }

    public void or(String param, final ExpressionEnum expr, final Object value) {
        WhereClauseElement element = new WhereClauseElement(param,expr,value);
        this.whereClause.add( element );
    }

    // join

    public void join(String param) {
        this.tempJoinTable = param;
        this.tempJoinType = JoinEnum.JOIN;
    }

    public void on(String param1, ExpressionEnum expression, String table2, String param2) {
        JoinClauseElement joinClauseElement =
                new JoinClauseElement(tempJoinTable,param1,tempJoinType, expression, table2, param2);
        if (this.joinClause == null) {
            this.joinClause = new ArrayList<>();
        }
        this.joinClause.add(joinClauseElement);
        this.tempJoinTable = null;
        this.tempJoinType = null;
    }

}
