package dk.ku.di.dms.vms.modb.iouring.util;

import java.io.File;

public class NativeLibraryLoader {
    private static boolean loadAttempted = false;

    public static synchronized void load() {
        OsVersionCheck.verifySystemRequirements();
        if (loadAttempted) {
            return;
        }
        loadAttempted = true;
        
        // Try multiple possible locations for the native library
        String[] possiblePaths = {
            "src/main/java/dk/ku/di/dms/vms/modb/iouring/c/lib/liburing_provider.so",
            "modb-iouring/src/main/java/dk/ku/di/dms/vms/modb/iouring/c/lib/liburing_provider.so",
            "./modb-iouring/src/main/java/dk/ku/di/dms/vms/modb/iouring/c/lib/liburing_provider.so"
        };
        
        File soFile = null;
        for (String path : possiblePaths) {
            File candidate = new File(path);
            if (candidate.exists()) {
                soFile = candidate;
                break;
            }
        }
        
        if (soFile == null) {
            StringBuilder errorMsg = new StringBuilder("Cannot find native library. Tried the following locations:\n");
            for (String path : possiblePaths) {
                errorMsg.append("  - ").append(new File(path).getAbsolutePath()).append("\n");
            }
            throw new IllegalStateException(errorMsg.toString());
        }
        
        System.load(soFile.getAbsolutePath());
    }
}
