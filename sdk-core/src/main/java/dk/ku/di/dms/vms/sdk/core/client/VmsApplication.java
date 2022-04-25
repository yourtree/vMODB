package dk.ku.di.dms.vms.sdk.core.client;

public class VmsApplication {

    // TODO ue invocation handler here to hide from application code
    public static void start(){

        // check first whether we are in decoupled or embed mode
        try {

            // Class<?> metadataLoaderEmbedClazz = Class.forName("VmsMetadataLoader");

            Class<?> metadataLoaderEmbedClazz = Class.forName("dk.ku.di.dms.vms.sdk.core.metadata.VmsMetadata");

            // IVmsMetadataLoader metadataLoader = metadataLoaderEmbedClazz.getClassLoader();

            // WebSocketHandlerBuilder.build( VmsSerdesProxyBuilder.build( null ), new VmsEventHandler(null) );

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

}
