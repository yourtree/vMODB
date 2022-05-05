package dk.ku.di.dms.vms.sidecar;

import dk.ku.di.dms.vms.sidecar.event.SidecarPubSub;
import dk.ku.di.dms.vms.sidecar.server.AsyncVMSServer;
import dk.ku.di.dms.vms.web_common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.web_common.serdes.VmsSerdesProxyBuilder;

public class Main {

    public static void main(String[] args){

        SidecarPubSub pubSub = SidecarPubSub.newInstance();
        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        AsyncVMSServer vmsServer = new AsyncVMSServer(pubSub.inputQueue, pubSub.outputQueue, serdes);
        new Thread(vmsServer).start();

    }

}
