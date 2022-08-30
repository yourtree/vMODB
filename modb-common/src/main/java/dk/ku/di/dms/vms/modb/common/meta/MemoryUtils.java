package dk.ku.di.dms.vms.modb.common.meta;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

public final class MemoryUtils {

    static {
        // initialize everything
        try {
            UNSAFE = getUnsafe();
            BUFFER_ADDRESS_FIELD_OFFSET =
                    getClassFieldOffset(Buffer.class, "address");
            BUFFER_CAPACITY_FIELD_OFFSET = getClassFieldOffset(Buffer.class, "capacity");
            DIRECT_BYTE_BUFFER_CLASS = getClassByName("java.nio.DirectByteBuffer");
        } catch (Exception ignored) {}

    }

    public static Unsafe UNSAFE;

    private static long BUFFER_ADDRESS_FIELD_OFFSET;
    private static long BUFFER_CAPACITY_FIELD_OFFSET;
    private static Class<?> DIRECT_BYTE_BUFFER_CLASS;

    private static long getClassFieldOffset(@SuppressWarnings("SameParameterValue")
                                                    Class<?> cl, String fieldName) throws NoSuchFieldException {
        return UNSAFE.objectFieldOffset(cl.getDeclaredField(fieldName));
    }


    public static Class<?> getClassByName(
            @SuppressWarnings("SameParameterValue") String className) throws ClassNotFoundException {

        return Class.forName(className);
    }

    // @SuppressWarnings("restriction")
    public static sun.misc.Unsafe getUnsafe() {
        Field unsafeField = null;
        try {
            unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            return (sun.misc.Unsafe) unsafeField.get(null);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    // if the byte buffer is not direct this class will throw error...
    public static void changeByteBufferAddress(ByteBuffer buffer, long newAddress){
        UNSAFE.putLong(buffer, BUFFER_ADDRESS_FIELD_OFFSET, newAddress);
    }

    public static ByteBuffer wrapUnsafeMemoryWithByteBuffer(long address, int size) {
        try {
            ByteBuffer buffer = (ByteBuffer) UNSAFE.allocateInstance(DIRECT_BYTE_BUFFER_CLASS);
            UNSAFE.putLong(buffer, BUFFER_ADDRESS_FIELD_OFFSET, address);
            UNSAFE.putInt(buffer, BUFFER_CAPACITY_FIELD_OFFSET, size);
            buffer.clear();
            return buffer;
        } catch (Throwable t) {
            throw new Error("Failed to wrap unsafe off-heap memory with ByteBuffer", t);
        }
    }

    public static long getByteBufferAddress(ByteBuffer buffer) {
        return UNSAFE.getLong(buffer, BUFFER_ADDRESS_FIELD_OFFSET);
    }

}
