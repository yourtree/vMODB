package dk.ku.di.dms.vms.modb.api.query.clause;

import dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum;

public class WhereClauseElement {
    private final String column;
    private final ExpressionTypeEnum expression;
    private Object value;

    public WhereClauseElement(String column, ExpressionTypeEnum expression, Object value) {
        this.column = column;
        this.expression = expression;
        this.value = value;
    }

    public String column() {
        return column;
    }

    public ExpressionTypeEnum expression() {
        return expression;
    }

    public Object value() {
        return value;
    }

    public void setValue(Object value){
        this.value = value;
    }

}

