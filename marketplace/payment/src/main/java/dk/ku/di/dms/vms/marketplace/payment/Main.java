package dk.ku.di.dms.vms.marketplace.payment;

import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;

public class Main {

    public static void main(String[] args) throws Exception {

        VmsApplication vms = VmsApplication.build("localhost", 8084, new String[]{
                "dk.ku.di.dms.vms.marketplace.payment",
                "dk.ku.di.dms.vms.marketplace.common"
        });
        vms.start();

    }

}
