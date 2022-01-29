package dk.ku.di.dms.vms.database.query.parser.enums;

public enum SortEnum {

    MIN("ASC"),
    MAX("DESC");

    public final String name;

    SortEnum(){ this.name = name(); }

    SortEnum(String name) {
        this.name = name;
    }

}
