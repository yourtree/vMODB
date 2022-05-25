package dk.ku.di.dms.vms.modb.storage;

import java.io.Serializable;

public final class EntryHeader implements Serializable {

    //   Boolean    - Active record indicator
    //   Byte       - Message type (0=Empty, 1=Bytes, 2=String)
    //   Integer    - Size of record's payload (not header)

    byte active;            // 1 byte
    byte type;              // 1 byte
    int size;               // 4 bytes

    public static final int HEADER_SIZE = Integer.BYTES  + (Byte.BYTES * 2);


}
