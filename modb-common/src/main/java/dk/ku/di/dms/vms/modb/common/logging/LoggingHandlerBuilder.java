package dk.ku.di.dms.vms.modb.common.logging;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Date;

public final class LoggingHandlerBuilder {

    public static ILoggingHandler build(String identifier) {
        String fileName = "logging_" + identifier + "_" + new Date().getTime() +".llog";
        Path path = Paths.get(fileName);
        FileChannel fileChannel;
        try {
            fileChannel = FileChannel.open(path,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ILoggingHandler handler;
        try {
            handler = new CompressedLoggingHandler(fileChannel, fileName);
        } catch (NoClassDefFoundError | Exception e) {
            System.out.println("Failed to load compressed logging handler: \n"+e);
            handler = new DefaultLoggingHandler(fileChannel, fileName);
        }
        return handler;
    }

}
