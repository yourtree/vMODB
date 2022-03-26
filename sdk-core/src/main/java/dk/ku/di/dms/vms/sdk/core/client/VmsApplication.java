package dk.ku.di.dms.vms.sdk.core.client;

import dk.ku.di.dms.vms.sdk.core.metadata.IVmsMetadataLoader;

public class VmsApplication {

    public static void start(){

        // check first whether we are in decoupled or embed mode
        try {

            // Class<?> metadataLoaderEmbedClazz = Class.forName("VmsMetadataLoader");

            Class<?> metadataLoaderEmbedClazz = Class.forName("dk.ku.di.dms.vms.sdk.core.metadata.VmsMetadata");

            // IVmsMetadataLoader metadataLoader = metadataLoaderEmbedClazz.getClassLoader();

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

}
