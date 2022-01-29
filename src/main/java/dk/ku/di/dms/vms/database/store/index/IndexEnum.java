package dk.ku.di.dms.vms.database.store.index;

public enum IndexEnum {

    BTREE("BTREE"),
    HASH("HASH");

    public final String name;

    IndexEnum(){ this.name = name(); }

    IndexEnum(String name) {
        this.name = name;
    }

}
