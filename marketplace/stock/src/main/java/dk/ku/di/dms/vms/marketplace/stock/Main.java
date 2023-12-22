package dk.ku.di.dms.vms.marketplace.stock;

import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;

public class Main {
    public static void main(String[] args) throws Exception {
        VmsApplication.build("localhost", 8082, new String[]{
                "dk.ku.di.dms.vms.marketplace.stock",
                "dk.ku.di.dms.vms.marketplace.common"
        });
    }
}