package dk.ku.di.dms.vms.marketplace.payment;

import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;

public final class Main {

    public static void main(String[] args) throws Exception {

        VmsApplicationOptions options = new VmsApplicationOptions("localhost", Constants.PAYMENT_VMS_PORT, new String[]{
                "dk.ku.di.dms.vms.marketplace.payment",
                "dk.ku.di.dms.vms.marketplace.common"
        }, 2, 1000);

        VmsApplication vms = VmsApplication.build(options);
        vms.start();

    }

}
