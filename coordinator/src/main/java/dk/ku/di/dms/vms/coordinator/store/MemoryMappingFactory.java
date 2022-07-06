package dk.ku.di.dms.vms.coordinator.store;

import dk.ku.di.dms.vms.coordinator.store.metadata.ServerMetadata;
import dk.ku.di.dms.vms.coordinator.store.metadata.MetadataAPI;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.logging.Logger;

/**
 * Class that encapsulates the complexities of dealing with memory-mapping files in Java
 */
public class MemoryMappingFactory {

    private static final Logger logger = Logger.getLogger("MemoryMappingFactory");

    public static MetadataAPI load(String dirPath, String fileName){
        return loadMemoryMappedFile(dirPath, fileName, Constants.DEFAULT_BUFFER_SIZE);
    }

    public static MetadataAPI load(){
        return loadMemoryMappedFile(
                Constants.DEFAULT_DIR_PATH,
                Constants.DEFAULT_FILENAME,
                Constants.DEFAULT_BUFFER_SIZE);
    }

    private static MetadataAPI loadMemoryMappedFile(String dirPath, String fileName, int bufferSize) {

        try {

            File dir = new File(dirPath);
            boolean created = dir.mkdir();
            if (!created) {
                if (!dir.exists()) {
                    logger.severe("Directory creation failed: " + dirPath);
                    return null;
                }
            }

            boolean fileExists = false;
            String filepath = dirPath + "/"+ fileName;
            try {
                File file = new File(fileName);
                fileExists = file.exists();
                if ( !fileExists ) {
                    File newFile = new File(filepath);
                    logger.info("Creating file " + fileName + " to " + newFile.getName());
                }
            }
            catch (NullPointerException ignored) { }

            RandomAccessFile raf = new RandomAccessFile( filepath, "rw" );

            FileChannel channel = raf.getChannel();
            ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);

            if ( !fileExists ) {
                raf.setLength(bufferSize);
                initialize(raf);
            } else {
                scan(raf);
            }

        } catch(NullPointerException | IOException e){ }

        return null;

    }

    /**
     * The structure of a coordinator metadata is as follows:
     *
     */
    private static void initialize(RandomAccessFile raf) throws IOException {

        // number of records
        raf.writeLong( 0L );

    }

    /**
     *
     * @param raf the file mapped in memory
     * @return
     */
    private static ServerMetadata scan(RandomAccessFile raf) {

        // TODO implement read after finishing the write methods
        // reading the committed offset, starts with 0
        return null;

    }

}
