package dk.ku.di.dms.vms.modb.schema.key;

/**
 * An interface for keys of rows and indexes
 */
public interface IKey {

    int hashCode();

    default boolean isSimple(){
        return false;
    }

}
