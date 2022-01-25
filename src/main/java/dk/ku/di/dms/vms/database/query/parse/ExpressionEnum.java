package dk.ku.di.dms.vms.database.query.parse;

public enum ExpressionEnum {

    LESS_THAN("<"),

    GREATER_THAN(">"),

    LESS_THAN_OR_EQUAL("<="),

    GREATER_THAN_OR_EQUAL(">="),

    EQUALS("="),

    NOT_EQUALS("<>"),

    LIKE("LIKE"),

    OR("OR"),

    AND("AND"),

    IN("IN"),

    NOT_IN("NOT IN"),

    NOT("NOT"),

    EXISTS("EXISTS"),

    IS_NULL("IS NULL"),

    IS_NOT_NULL("IS NOT NULL");

    public final String name;

    ExpressionEnum(){ this.name = name(); }

    ExpressionEnum(String name) {
        this.name = name;
    }

}
