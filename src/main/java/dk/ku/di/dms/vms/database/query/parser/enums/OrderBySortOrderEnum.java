package dk.ku.di.dms.vms.database.query.parser.enums;

public enum OrderBySortOrderEnum {

    MIN("ASC"),
    MAX("DESC");

    public final String name;

    OrderBySortOrderEnum(){ this.name = name(); }

    OrderBySortOrderEnum(String name) {
        this.name = name;
    }

}
