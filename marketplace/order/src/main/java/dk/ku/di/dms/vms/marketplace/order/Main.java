package dk.ku.di.dms.vms.marketplace.order;

import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.marketplace.common.Utils;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;

import java.util.Properties;

public final class Main {

    public static void main(String[] args) throws Exception {
        Properties properties = Utils.loadProperties();
        int networkBufferSize = Integer.parseInt( properties.getProperty("network_buffer_size") );
        int networkThreadPoolSize = Integer.parseInt( properties.getProperty("network_thread_pool_size") );
        long consumerSendRate = Long.parseLong( properties.getProperty("consumer_send_rate") );

        VmsApplicationOptions options = new VmsApplicationOptions("localhost", Constants.ORDER_VMS_PORT, new String[]{
                "dk.ku.di.dms.vms.marketplace.order",
                "dk.ku.di.dms.vms.marketplace.common"
        }, networkBufferSize == 0 ? MemoryUtils.DEFAULT_PAGE_SIZE : networkBufferSize,
                networkThreadPoolSize, consumerSendRate);

        VmsApplication vms = VmsApplication.build(options);
        vms.start();
    }
}