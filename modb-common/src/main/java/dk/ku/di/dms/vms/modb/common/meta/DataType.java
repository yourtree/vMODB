package dk.ku.di.dms.vms.modb.common.meta;

import static dk.ku.di.dms.vms.modb.common.meta.Constants.DEFAULT_MAX_SIZE_CHAR;

public enum DataType {

    // used for boolean
    BYTE(Byte.BYTES),

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
