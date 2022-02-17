package dk.ku.di.dms.vms.database.query.parser.enums;

public enum ExpressionTypeEnum {

    // value expression

    LESS_THAN("<"),

    GREATER_THAN(">"),

    LESS_THAN_OR_EQUAL("<="),

    GREATER_THAN_OR_EQUAL(">="),

    EQUALS("="),

    NOT_EQUALS("<>"),

    IS_NULL("IS NULL"),

    IS_NOT_NULL("IS NOT NULL"),

    // only for string or char
    LIKE("LIKE"),

    // boolean expression

    OR("OR"),

    AND("AND"),

    // set expression

    IN("IN"),

    NOT_IN("NOT IN"),

    NOT("NOT"),

    EXISTS("EXISTS");

    public final String name;

    ExpressionTypeEnum(){ this.name = name(); }

    ExpressionTypeEnum(String name) {
        this.name = name;
    }

}
