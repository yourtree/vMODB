package dk.ku.di.dms.vms.database.store;

public abstract class AbstractIndex {

    // use bitwise comparison to find whether a given index exists for such columns
    // https://stackoverflow.com/questions/8504288/java-bitwise-comparison-of-a-byte/8504393
    public abstract int hashCode();

    // clustered inde does not make sense in mmdbs

}
