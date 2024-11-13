package dk.ku.di.dms.vms.tpcc.order;

import dk.ku.di.dms.vms.sdk.embed.client.DefaultHttpHandler;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;

/**
 * Port of the TPC-C order-related code as a virtual microservice
 */
public final class Main {
    public static void main( String[] args ) throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build(
                "0.0.0.0",
                8003, new String[]{
                        "dk.ku.di.dms.vms.tpcc.order-tpcc",
                        "dk.ku.di.dms.vms.tpcc.common-tpcc"
                });
        VmsApplication vms = VmsApplication.build(options, (x, ignored) -> new DefaultHttpHandler(x));
        vms.start();
    }
}
