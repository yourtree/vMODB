package dk.ku.di.dms.vms.web_common.iouring.util;

public class ManualNativeLoaderTest {
    public static void main(String[] args) {
        try {
            System.out.println("Trying to load native library...");
            NativeLibraryLoader.load();
            System.out.println("✅ Native library loaded successfully!");
        } catch (Throwable t) {
            System.err.println("❌ Failed to load native library:");
            t.printStackTrace();
            System.exit(1);
        }
    }
}
