package dk.ku.di.dms.vms.marketplace.order;

import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;

public final class Main {
    public static void main(String[] args) throws Exception {

        VmsApplication vms = VmsApplication.build("localhost", 8083, new String[]{
                "dk.ku.di.dms.vms.marketplace.order",
                "dk.ku.di.dms.vms.marketplace.common"
        });
        vms.start();
    }
}