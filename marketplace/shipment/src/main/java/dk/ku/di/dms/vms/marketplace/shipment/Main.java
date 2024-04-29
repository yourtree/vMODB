package dk.ku.di.dms.vms.marketplace.shipment;

import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;

public final class Main {

    public static void main(String[] args) throws Exception {

        VmsApplicationOptions options = new VmsApplicationOptions("localhost", Constants.SHIPMENT_VMS_PORT, new String[]{
                "dk.ku.di.dms.vms.marketplace.shipment",
                "dk.ku.di.dms.vms.marketplace.common"
        }, 4096, 2);

        VmsApplication vms = VmsApplication.build(options);
        vms.start();
    }

}
