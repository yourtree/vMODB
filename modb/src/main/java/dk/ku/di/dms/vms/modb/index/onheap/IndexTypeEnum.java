package dk.ku.di.dms.vms.modb.index.onheap;

public enum IndexTypeEnum {

    UNIQUE, // for primary key, can be single or multiple column

    // non-unique

    HASH, // non-unique hash-based column set identifies several table

    // RANGE, FILTER
    RANGE
}
