package dk.ku.di.dms.vms.database.query.parser.enums;

public enum GroupByAggregateFunctionEnum {

    AVG("AVG"),
    SUM("SUM"),
    COUNT("COUNT"),
    MIN("MIN"),
    MAX("MAX");

    public final String name;

    GroupByAggregateFunctionEnum(){ this.name = name(); }

    GroupByAggregateFunctionEnum(String name) {
        this.name = name;
    }

}
