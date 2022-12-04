package dk.ku.di.dms.vms.modb.definition.key;

public class IntKey implements IKey {

    public int value;

    // private constructor
    private IntKey() {}

    public static IntKey of() {
        return new IntKey();
    }

    public void newValue(int newValue){
        this.value = newValue;
    }

    @Override
    public int hashCode(){
        return this.value;
    }

    @Override
    public boolean equals(Object object){
        return this.hashCode() == object.hashCode();
    }

}
