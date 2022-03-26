package dk.ku.di.dms.vms.modb.common.query.enums;

public enum GroupByOperationEnum {

    AVG("AVG"),
    SUM("SUM"),
    COUNT("COUNT"),
    MIN("MIN"),
    MAX("MAX");

    public final String name;

    GroupByOperationEnum(){ this.name = name(); }

    GroupByOperationEnum(String name) {
        this.name = name;
    }

}
