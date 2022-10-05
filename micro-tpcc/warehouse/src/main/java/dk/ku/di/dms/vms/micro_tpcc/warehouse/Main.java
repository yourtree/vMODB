package dk.ku.di.dms.vms.micro_tpcc.warehouse;

import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;

public class Main {

    public static void main(String[] args){
        VmsApplication.start("localhost", 8081, new String[]{
                "dk.ku.di.dms.vms.micro_tpcc.common",
                "dk.ku.di.dms.vms.micro_tpcc.warehouse"
        },
            "customer","stock","item"   );
    }

}
