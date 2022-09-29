package dk.ku.di.dms.vms.sdk.embed.client;

import dk.ku.di.dms.vms.modb.common.schema.network.VmsIdentifier;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.embed.channel.VmsEmbedInternalChannels;
import dk.ku.di.dms.vms.sdk.embed.facade.ModbModules;
import dk.ku.di.dms.vms.sdk.embed.metadata.EmbedMetadataLoader;
import dk.ku.di.dms.vms.sdk.embed.scheduler.EmbedVmsTransactionScheduler;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

/**
 * Starting point for initializing the runtime
 */
public final class VmsApplication {

    private static final Logger logger = Logger.getLogger("VmsApplication");

    public static void start(String host, int port){

        // check first whether we are in decoupled or embed mode
        try {

            Optional<Package> optional = Arrays.stream(Package.getPackages()).filter(p ->
                             !p.getName().contentEquals("dk.ku.di.dms.vms.sdk.embed")
                          && !p.getName().contentEquals("dk.ku.di.dms.vms.sdk.core")
                          && !p.getName().contentEquals("dk.ku.di.dms.vms.modb")
                          && !p.getName().contains("java")
                          && !p.getName().contains("sun")
                          && !p.getName().contains("jdk")
                          && !p.getName().contains("com")
                          && !p.getName().contains("org")
                                                        ).findFirst();

            String packageName = optional.map(Package::getName).orElse("Nothing");
            logger.info(packageName);

            if(packageName.equalsIgnoreCase("Nothing")) throw new IllegalStateException("Cannot identify package.");

            VmsEmbedInternalChannels vmsInternalPubSubService = new VmsEmbedInternalChannels();

            VmsRuntimeMetadata vmsMetadata = EmbedMetadataLoader.loadRuntimeMetadata(packageName);

            ModbModules modbModules = EmbedMetadataLoader.loadModbModulesIntoRepositories(vmsMetadata);

            assert vmsMetadata != null;

            ExecutorService readTaskPool = Executors.newSingleThreadExecutor();

            IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

            // for now only giving support to one vms
            String vmsName = vmsMetadata.loadedVmsInstances().entrySet().stream().findFirst().get().getKey();

            EmbedVmsTransactionScheduler scheduler =
                    new EmbedVmsTransactionScheduler(
                            readTaskPool,
                            vmsInternalPubSubService,
                            vmsMetadata.queueToVmsTransactionMap(),
                            vmsMetadata.queueToEventMap(),
                            serdes,
                            modbModules.catalog());

            VmsIdentifier vmsIdentifier = new VmsIdentifier(
                    host, port, vmsName,
                    0, 0,
                    vmsMetadata.dataSchema(),
                    vmsMetadata.inputEventSchema(), vmsMetadata.outputEventSchema());

            // TODO setup server to receive data, bulk loading
            // must pass the corresponding repositories

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
