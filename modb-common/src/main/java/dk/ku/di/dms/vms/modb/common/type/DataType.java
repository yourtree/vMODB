package dk.ku.di.dms.vms.modb.common.type;

import static dk.ku.di.dms.vms.modb.common.type.Constants.DEFAULT_MAX_SIZE_CHAR;

public enum DataType {

    BOOL(0), // actually 1 bit, not 1 byte

    INT(Integer.BYTES),

    CHAR(Character.BYTES * DEFAULT_MAX_SIZE_CHAR),

    LONG(Long.BYTES),

    FLOAT(Float.BYTES),

    DOUBLE(Double.BYTES),

    DATE(Long.BYTES);

    public final int value;

    DataType(int bytes) {
        this.value = bytes;
    }
}
