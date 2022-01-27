package dk.ku.di.dms.vms.database.query.parser.stmt;

public enum SortOperationEnum {

    MIN("MIN"),
    MAX("MAX");

    public final String name;

    SortOperationEnum(){ this.name = name(); }

    SortOperationEnum(String name) {
        this.name = name;
    }

}
