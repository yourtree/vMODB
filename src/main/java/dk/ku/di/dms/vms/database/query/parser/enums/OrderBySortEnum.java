package dk.ku.di.dms.vms.database.query.parser.enums;

public enum OrderBySortEnum {

    MIN("ASC"),
    MAX("DESC");

    public final String name;

    OrderBySortEnum(){ this.name = name(); }

    OrderBySortEnum(String name) {
        this.name = name;
    }

}
