package dk.ku.di.dms.vms.database.query.parser.clause;

import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionTypeEnum;

public class HavingClauseElement<T extends Number> {

    public final ExpressionTypeEnum expression;
    public final T value;

    public HavingClauseElement(ExpressionTypeEnum expression, T value) {
        this.expression = expression;
        this.value = value;
    }

}
