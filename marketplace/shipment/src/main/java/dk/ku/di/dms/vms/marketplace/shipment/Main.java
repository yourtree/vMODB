package dk.ku.di.dms.vms.marketplace.shipment;

import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;

public final class Main {

    public static void main(String[] args) throws Exception {
        VmsApplication vms = VmsApplication.build("localhost", Constants.SHIPMENT_PORT, new String[]{
                "dk.ku.di.dms.vms.marketplace.shipment",
                "dk.ku.di.dms.vms.marketplace.common"
        });
        vms.start();
    }

}
