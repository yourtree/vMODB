package dk.ku.di.dms.vms.modb.common.logging;

import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Date;

public final class LoggingHandlerBuilder {

    private static final String IOURING_ENABLED_PROPERTY = "iouring.enabled";
    private static final String LOGGING_TYPE_PROPERTY = "logging_type";

    public static ILoggingHandler build(String identifier) {
        String fileName = identifier + "_" + new Date().getTime() +".llog";
        String userHome = ConfigUtils.getUserHome();
        String basePath = userHome + "/vms";
        File theDir = new File(basePath);
        assert theDir.exists() || theDir.mkdirs();
        String filePath = basePath + "/" + fileName;
        Path path = Paths.get(filePath);
        
        // Determine logging type based on configuration
        boolean ioUringEnabled = Boolean.parseBoolean(System.getProperty(IOURING_ENABLED_PROPERTY, "false"));
        String loggingType = System.getProperty(LOGGING_TYPE_PROPERTY, "default");
        
        // For IoUring logging types, try to create IoUring handlers
        if (ioUringEnabled && ("iouring".equals(loggingType) || "compressed_iouring".equals(loggingType))) {
            try {
                if ("compressed_iouring".equals(loggingType)) {
                    return new CompressedIoUringLoggingHandler(identifier);
                } else {
                    return new IoUringLoggingHandler(identifier);
                }
            } catch (Exception e) {
                System.err.println("Failed to create IoUring LoggingHandler, falling back to standard implementation: " + e.getMessage());
            }
        }
        
        // Fallback to standard file-based implementations
        FileChannel fileChannel;
        try {
            fileChannel = FileChannel.open(path,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        // Create standard handlers based on configuration
        if ("compressed".equals(loggingType) || "compressed_iouring".equals(loggingType)) {
            return new CompressedLoggingHandler(fileChannel, fileName);
        } else {
            return new DefaultLoggingHandler(fileChannel, fileName);
        }
    }
}
