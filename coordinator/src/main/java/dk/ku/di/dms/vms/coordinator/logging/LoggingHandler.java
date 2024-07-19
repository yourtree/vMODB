package dk.ku.di.dms.vms.coordinator.logging;

import dk.ku.di.dms.vms.modb.common.transaction.ILoggingHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Date;

public class LoggingHandler implements ILoggingHandler {

    private final FileChannel fileChannel;
    private final String fileName;

    private LoggingHandler(FileChannel channel, String fileName) {
        this.fileChannel = channel;
        this.fileName = fileName;
    }

    public static LoggingHandler build(String identifier) {
        var dt = new Date();
        var fileName = "logging_" + identifier + "_"+ dt.getTime() +".txt";
        Path path = Paths.get(fileName);
        try {
            var fileChannel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE,
                StandardOpenOption.DSYNC);
            return new LoggingHandler(fileChannel, fileName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close(){
        try {
            this.fileChannel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void log(ByteBuffer byteBuffer) throws IOException {
        int pos;
        do {
            pos = this.fileChannel.write(byteBuffer);
        } while(pos < byteBuffer.limit());
    }

    public String getFileName() {
        return this.fileName;
    }
}
