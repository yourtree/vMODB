package dk.ku.di.dms.vms.modb.storage.infra;

public final class MemoryManagerBuilder {

    private MemoryManagerBuilder(){}

    public static MemoryManager build(){
        return new MemoryManager();
    }

}
