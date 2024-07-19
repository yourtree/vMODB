package dk.ku.di.dms.vms.modb.common.transaction;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface ILoggingHandler {

    default void log(ByteBuffer byteBuffer) throws IOException {}

    default void close() { }
}
