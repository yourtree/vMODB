package dk.ku.di.dms.vms.database.query.parser.enums;

public enum StatementEnum {

    SELECT("SELECT"),

    INSERT_INTO("INSERT INTO"),

    VALUES("VALUES"),

    DELETE("DELETE"),

    UPDATE("UPDATE"),

    ORDER_BY("ORDER BY"),

    GROUP_BY("GROUP BY"),

    SET("SET"),

    AS("AS"),

    WHERE("WHERE"),

    FROM("FROM");

    public final String name;

    StatementEnum(){ this.name = name(); }

    StatementEnum(String name) {
        this.name = name;
    }

}
