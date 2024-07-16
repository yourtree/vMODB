package dk.ku.di.dms.vms.coordinator.logging;

import dk.ku.di.dms.vms.modb.common.transaction.ILoggingHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class LoggingHandler implements ILoggingHandler {

    private final FileChannel fileChannel;

    public LoggingHandler() throws IOException {
        var threadId = Thread.currentThread().threadId();
        Path path = Paths.get("logging_" + threadId + ".txt");
        this.fileChannel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE,
                StandardOpenOption.DSYNC);
    }

    public void log(ByteBuffer byteBuffer) throws IOException {
        byteBuffer.position(0);
        int pos;
        do {
            pos = this.fileChannel.write(byteBuffer);
        } while(pos < byteBuffer.limit());
    }

}
