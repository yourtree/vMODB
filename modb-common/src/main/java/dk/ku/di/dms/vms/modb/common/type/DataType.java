package dk.ku.di.dms.vms.modb.common.type;

public enum DataType {

    BOOL(0), // actually 1 bit, not 1 byte

    INT(Integer.BYTES),

    CHAR(Character.BYTES),

    // stored as char array of fixed size
    STRING(Character.BYTES * Constants.DEFAULT_MAX_SIZE_STRING),

    LONG(Long.BYTES),

    FLOAT(Float.BYTES),

    DOUBLE(Double.BYTES),

    DATE(Long.BYTES),

    // should only be used by events
    STRING_ARRAY(Constants.DEFAULT_MAX_SIZE_STRING),

    FLOAT_ARRAY(Float.BYTES * 10),

    INT_ARRAY(Integer.BYTES * 10),

    ENUM(Constants.DEFAULT_MAX_SIZE_STRING);

    public final int value;

    DataType(int bytes) {
        this.value = bytes;
    }
}
