package dk.ku.di.dms.vms.database.store.index;

public enum IndexDataStructureEnum {

    TREE("TREE"),
    HASH("HASH");

    public final String name;

    IndexDataStructureEnum(){ this.name = name(); }

    IndexDataStructureEnum(String name) {
        this.name = name;
    }

}
