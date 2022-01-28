package dk.ku.di.dms.vms.database.query.parser.enums;

public enum GroupEnum {

    AVG("AVG"),
    COUNT("COUNT"),
    MIN("MIN"),
    MAX("MAX");

    public final String name;

    GroupEnum(){ this.name = name(); }

    GroupEnum(String name) {
        this.name = name;
    }

}
