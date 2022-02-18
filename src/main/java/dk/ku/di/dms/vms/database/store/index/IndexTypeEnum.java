package dk.ku.di.dms.vms.database.store.index;

public enum IndexTypeEnum {

    SIMPLE("SIMPLE"),
    COMPOSITE("COMPOSITE");

    public final String name;

    IndexTypeEnum(){ this.name = name(); }

    IndexTypeEnum(String name) {
        this.name = name;
    }

}
