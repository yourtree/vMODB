package dk.ku.di.dms.vms.marketplace.payment;

import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;

public final class Main {

    public static void main(String[] args) throws Exception {

        VmsApplication vms = VmsApplication.build("localhost", Constants.PAYMENT_VMS_PORT, new String[]{
                "dk.ku.di.dms.vms.marketplace.payment",
                "dk.ku.di.dms.vms.marketplace.common"
        });
        vms.start();

    }

}
