package dk.ku.di.dms.vms.database.store.index;

public enum IndexTypeEnum {

    TREE("TREE"),
    HASH("HASH");

    public final String name;

    IndexTypeEnum(){ this.name = name(); }

    IndexTypeEnum(String name) {
        this.name = name;
    }

}
