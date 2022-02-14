package dk.ku.di.dms.vms.database.query.parser.enums;

public enum GroupByEnum {

    AVG("AVG"),
    SUM("SUM"),
    COUNT("COUNT"),
    MIN("MIN"),
    MAX("MAX");

    public final String name;

    GroupByEnum(){ this.name = name(); }

    GroupByEnum(String name) {
        this.name = name;
    }

}
