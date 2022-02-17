package dk.ku.di.dms.vms.database.query.parser.enums;

public enum JoinTypeEnum {

    JOIN("JOIN"),

    LEFT_JOIN("LEFT JOIN"),

    RIGHT_JOIN("RIGHT JOIN"),

    INNER_JOIN("INNER JOIN"),

    FULL_OUTER_INSERT("FULL OUTER JOIN");

    public final String name;

    JoinTypeEnum(){ this.name = name(); }

    JoinTypeEnum(String name) {
        this.name = name;
    }

}
