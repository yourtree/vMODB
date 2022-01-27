package dk.ku.di.dms.vms.database.query.parser.stmt;

public enum JoinEnum {

    JOIN("JOIN"),

    LEFT_JOIN("LEFT JOIN"),

    RIGHT_JOIN("RIGHT JOIN"),

    INNER_JOIN("INNER JOIN"),

    FULL_OUTER_INSERT("FULL OUTER JOIN");

    public final String name;

    JoinEnum(){ this.name = name(); }

    JoinEnum(String name) {
        this.name = name;
    }

}
