package dk.ku.di.dms.vms.modb.index.onheap;

public enum IndexDataStructureEnum {

    TREE("TREE"),
    HASH("HASH");

    public final String name;

    IndexDataStructureEnum(){ this.name = name(); }

    IndexDataStructureEnum(String name) {
        this.name = name;
    }

}
