package dk.ku.di.dms.vms.modb.schema;

/**
 * The header of an entry in a buffer
 */
public class Header {

    static final int SIZE = Byte.BYTES;
    public static final byte active = 1;
    public static final byte inactive = 0;

}