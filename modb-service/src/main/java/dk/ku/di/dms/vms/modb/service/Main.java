package dk.ku.di.dms.vms.modb.service;

import dk.ku.di.dms.vms.modb.service.event.SidecarPubSub;
import dk.ku.di.dms.vms.modb.service.server.AsyncVMSServer;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;

public class Main {

    public static void main(String[] args){

        SidecarPubSub pubSub = SidecarPubSub.newInstance();
        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        AsyncVMSServer vmsServer = new AsyncVMSServer(pubSub.inputQueue, pubSub.outputQueue, serdes);
        new Thread(vmsServer).start();

    }

}
