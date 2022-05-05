package dk.ku.di.dms.vms.sdk.core.client;

import dk.ku.di.dms.vms.sdk.core.manager.VmsManager;

/**
 * Starting point for initializing the runtime
 */
public class VmsApplication {

    // TODO ue invocation handler here to hide from application code
    public static void start(){

        // check first whether we are in decoupled or embed mode
        try {

            VmsManager vmsManager = new VmsManager()

            // Class<?> metadataLoaderEmbedClazz = Class.forName("VmsMetadataLoader");

            Class<?> metadataLoaderEmbedClazz = Class.forName("dk.ku.di.dms.vms.sdk.core.metadata.VmsMetadata");

            // IVmsMetadataLoader metadataLoader = metadataLoaderEmbedClazz.getClassLoader();

            // WebSocketHandlerBuilder.build( VmsSerdesProxyBuilder.build( null ), new VmsEventHandler(null) );

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

}
