package dk.ku.di.dms.vms.database.store.index;

public enum IndexKeyTypeEnum {

    SIMPLE("SIMPLE"),
    COMPOSITE("COMPOSITE");

    public final String name;

    IndexKeyTypeEnum(){ this.name = name(); }

    IndexKeyTypeEnum(String name) {
        this.name = name;
    }

}
