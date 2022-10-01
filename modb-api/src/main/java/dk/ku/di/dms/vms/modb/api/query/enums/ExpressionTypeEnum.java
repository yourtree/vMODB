package dk.ku.di.dms.vms.modb.api.query.enums;

public enum ExpressionTypeEnum {

    // value expression

    LESS_THAN("<"),

    GREATER_THAN(">"),

    LESS_THAN_OR_EQUAL("<="),

    GREATER_THAN_OR_EQUAL(">="),

    EQUALS("="),

    NOT_EQUALS("<>"),

    // only for string or char
    LIKE("LIKE"),

    // nullable expression

    IS_NULL("IS NULL"),

    IS_NOT_NULL("IS NOT NULL"),

    // boolean expression

    OR("OR"),

    AND("AND"),

    // set expression

    IN("IN"),

    NOT_IN("NOT IN"),

    NOT("NOT"),

    EXISTS("EXISTS");

    public final String name;

    ExpressionTypeEnum(String name) {
        this.name = name;
    }

}
