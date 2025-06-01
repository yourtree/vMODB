package dk.ku.di.dms.vms.web_common.iouring;

import dk.ku.di.dms.vms.web_common.iouring.util.NativeLibraryLoader;

/**
 * An {@link AbstractIoUringChannel} implementation for file operations.
 */
public class IoUringFile extends AbstractIoUringChannel {

    /**
     * Instantiates a new {@code IoUringFile}.
     *
     * @param path The path to the file
     */
    public IoUringFile(String path) {
        super(IoUringFile.open(path));
    }

    private static native int open(String path);

    static {
        NativeLibraryLoader.load();
    }
}
