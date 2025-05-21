/**
 * NativeLibraryLoader.java
 * 
 * Utility class to load the native io_uring library.
 */
package dk.ku.di.dms.vms.sdk.embed.iouring;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

/**
 * Utility class to load the native io_uring library.
 */
public final class NativeLibraryLoader {
    
    private static final String LIBRARY_NAME = "vms-iouring";
    private static final String TEMP_DIR_PREFIX = "vms-iouring-native-";
    private static boolean loaded = false;
    
    private NativeLibraryLoader() {
        // Prevent instantiation
    }
    
    /**
     * Loads the native library.
     * 
     * @throws UnsatisfiedLinkError if the library cannot be loaded
     */
    public static synchronized void load() {
        if (loaded) {
            return;
        }
        
        try {
            // Try to load from java.library.path
            System.loadLibrary(LIBRARY_NAME);
            loaded = true;
            return;
        } catch (UnsatisfiedLinkError e) {
            // Try to extract and load from the JAR
            try {
                loadFromJar();
                loaded = true;
                return;
            } catch (Exception ex) {
                // Fall through to the error message
                ex.printStackTrace();
            }
        }
        
        throw new UnsatisfiedLinkError(
                "Failed to load native library: " + LIBRARY_NAME + 
                ". Make sure the library is in your java.library.path " +
                "or included in the JAR file."
        );
    }
    
    /**
     * Checks if the native library is loaded.
     * 
     * @return true if the native library is loaded, false otherwise
     */
    public static boolean isLoaded() {
        return loaded;
    }
    
    /**
     * Extracts and loads the native library from the JAR file.
     * 
     * @throws IOException if an I/O error occurs
     */
    private static void loadFromJar() throws IOException {
        // Get the architecture-specific library name
        String platformLib = getPlatformLibraryName();
        
        // Get the resource path
        String resourcePath = "/native/" + getArchitectureName() + "/" + platformLib;
        
        // Create a temporary directory
        Path tempDir = Files.createTempDirectory(TEMP_DIR_PREFIX);
        tempDir.toFile().deleteOnExit();
        
        // Extract the library to the temporary directory
        File libFile = new File(tempDir.toFile(), platformLib);
        libFile.deleteOnExit();
        
        // Copy the library from the JAR to the temporary file
        try (InputStream in = NativeLibraryLoader.class.getResourceAsStream(resourcePath);
             FileOutputStream out = new FileOutputStream(libFile)) {
            
            if (in == null) {
                throw new IOException("Native library not found in JAR: " + resourcePath);
            }
            
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        }
        
        // Load the library
        System.load(libFile.getAbsolutePath());
    }
    
    /**
     * Gets the platform-specific library name.
     * 
     * @return the platform-specific library name
     */
    private static String getPlatformLibraryName() {
        String osName = System.getProperty("os.name").toLowerCase(Locale.US);
        
        if (osName.contains("linux")) {
            return "lib" + LIBRARY_NAME + ".so";
        } else if (osName.contains("mac") || osName.contains("darwin")) {
            return "lib" + LIBRARY_NAME + ".dylib";
        } else if (osName.contains("windows")) {
            return LIBRARY_NAME + ".dll";
        } else {
            throw new UnsatisfiedLinkError("Unsupported operating system: " + osName);
        }
    }
    
    /**
     * Gets the architecture name for the current platform.
     * 
     * @return the architecture name
     */
    private static String getArchitectureName() {
        String osName = System.getProperty("os.name").toLowerCase(Locale.US);
        String arch = System.getProperty("os.arch").toLowerCase(Locale.US);
        
        if (osName.contains("linux")) {
            if (arch.contains("amd64") || arch.contains("x86_64")) {
                return "linux_x86-64";
            } else if (arch.contains("aarch64") || arch.contains("arm64")) {
                return "linux_aarch64";
            }
        } else if (osName.contains("mac") || osName.contains("darwin")) {
            if (arch.contains("aarch64") || arch.contains("arm64")) {
                return "macos_aarch64";
            } else {
                return "macos_x86-64";
            }
        } else if (osName.contains("windows")) {
            return "windows_x86-64";
        }
        
        throw new UnsatisfiedLinkError("Unsupported platform: " + osName + " " + arch);
    }
} 